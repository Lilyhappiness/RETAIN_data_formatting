from copy import deepcopy
from math import isclose
from operator import itemgetter

class PID_Counter():
    def __init__(self, pid=None, day=0, preferred_types=['ED', 'IP']):
        self.event_types = ['ED', 'IP']
        upper_ptypes = [ptype.upper() for ptype in preferred_types if ptype]
        if not all(ptype in self.event_types for ptype in upper_ptypes):
            raise ValueError("Outcome types must be subset of ({})".format(", ".join(self.event_types)))
        self.preferred_event_types = upper_ptypes
        self.pid = pid
        self.day = day
        self.prev_pid = None
        self.prev_day = None
        self.pid_rows = []
        self.duped_rows = []
        self.is_wide_version = False
        self.prev_event_value = 0
        self.pid_event_count = 0
        self.partial_events = {}
                
    def reset_for_new_pid(self, new_pid):
        self.prev_pid = self.pid
        self.pid = new_pid
        self.pid_rows = []
        self.duped_rows = []
        self.is_wide_version = False
        self.prev_event_value = 0
        self.pid_event_count = 0
        self.partial_events = {}

    def is_new_pid(self, new_pid):
        '''
        returns true if the passed in PID is different from the internal PID
        
        >>> pc = PID_Counter(event_type='ED')
        >>> pc.pid = 1
        >>> pc.is_new_pid(2)
        True
        >>> pc.pid = 170300038084.0
        >>> pc.is_new_pid(170300038084.0)
        False
        >>> pc.pid = "ten"
        >>> pc.is_new_pid("ten")
        False
        
        '''
        return (str(self.pid) != str(new_pid)) if self.pid else True
        
    def process_row(self, visit_row):
        self.prev_day = self.day
        self.prev_pid = self.pid
        self.pid = visit_row['PID']
        self.day = visit_row['DAY']
        if self._is_new_event(visit_row):
            self.pid_event_count += 1
            self._dupe_events(visit_row.get('DAY'))
        self.pid_rows.append(visit_row)
        self.prev_event_value = 1 if any(visit_row.get(ptype) for ptype in self.preferred_event_types) else 0

    def _is_new_event(self, visit_row):
        # adding in "non subsequent day filter" - this deals with the case where patients have 
        # an outcome event, then one or more days with no event AND no claims, followed by a separate outcome event
        # as well as creating new events for each active day, for the day_instance/test dataset creation
        return  (any(visit_row[event] is 1 for event in self.preferred_event_types) and\
                ((self.prev_event_value is 0) or 
                (self.prev_event_value is 1 and (self.day - self.prev_day) > 1)))
    
    def _dupe_events(self, day=None):
        '''
        Use the total number of encountered events to create 
        a duplicate of all rows to this point with a namespace-separated PID
        This is saved to a list-of-lists to be written out at a later point
        >>> pc = PID_Counter(event_type='ED')
        >>> pc.pid = 1
        >>> pc.pid_event_count['ED'] = 1
        >>> pc.pid_rows.append({'PID': 1, 'ED':0, 'IP':1, 'DAY':1 })
        >>> pc.pid_rows.append({'PID': 1, 'ED':1, 'IP':1, 'DAY':2 })
        >>> pc.pid_rows.append({'PID': 1, 'ED':0, 'IP':1, 'DAY':3 })
        >>> pc._dupe_events()
        >>> from  pprint import pprint
        >>> pprint(pc.duped_rows)
        [[{'DAY': 1, 'ED': 0, 'IP': 1, 'PID': '1_1'},
          {'DAY': 2, 'ED': 1, 'IP': 1, 'PID': '1_1'},
          {'DAY': 3, 'ED': 0, 'IP': 1, 'PID': '1_1'}]]
        
        '''
        new_PID = "_".join([str(self.pid), str(self.pid_event_count)])
        duped_rows = deepcopy(self.pid_rows)
        # add partial_event visit row for event day
        if day and self.partial_events.get(day):
            duped_rows.append(self.partial_events[day])
        #set new PID for duped rows
        for row in duped_rows:
            row['PID'] = new_PID
        self.duped_rows.append(duped_rows)

    def convert_rows_to_wide_rep(self):
        if self.is_wide_version is True or not self.pid_rows:
            print("Wide Conversion not possible for this PID")
            return
        
        def _make_wide_rows(row_list):
            '''
            return a tuple of:
              condensed numerics list (a list-of-lists, 1 per day)
              condensed codes list (a list of all codes that appear at least once)
            '''
            wide_row_numerics = []
            wide_row_codes = []
            wide_row_days = []
            sorted_row_list = sorted(row_list, key=itemgetter('PID', 'DAY'))
            for row in row_list:
                wide_row_numerics.append(row['numerics'])
                wide_row_codes.append(row['codes'])
                wide_row_days.append(row['DAY'])

                
            return wide_row_numerics, wide_row_codes, wide_row_days

        def _create_condensed_row(pid, row_list, event_target):
            condensed_numerics, condensed_codes, condensed_days = _make_wide_rows(row_list)
            row = [{'PID': pid,
                    'numerics': condensed_numerics,
                    'codes': condensed_codes,
                    'to_event': condensed_days,
                    'target': event_target
                    }]

            return row


        #convert vanilla rows
        self.pid_rows = _create_condensed_row(self.pid, self.pid_rows, self.prev_event_value)
        #convert duped rows (a list of lists)
        condensed_duped_rows = []
        for duped_pid_chunk in self.duped_rows:
            if not duped_pid_chunk:
                continue
            duped_row = _create_condensed_row(duped_pid_chunk[0]['PID'], duped_pid_chunk, 1)
            condensed_duped_rows.append(duped_row)
            
        self.duped_rows = condensed_duped_rows
        self.is_wide_version = True

        
    def write_all_rows(self, writer, neg_only=True):
        '''
        write all duped event rows to writer obj
        write full PID event row to writer obj if target is 0
        '''
        for dupes in self.duped_rows:
            if dupes: #duped_rows can either be an empty list or a list of empty lists. guard against the latter
                writer.writerows(dupes)
        if self.pid_rows and (not neg_only or (self.pid_rows[0]['target'] == 0)):
            writer.writerows(self.pid_rows)

    def convert_and_write(self, csv_writer, append=False, visit=None, neg_only=True):
        if append:
            self.pid_rows.append(visit)
        self.convert_rows_to_wide_rep()
        self.write_all_rows(csv_writer, neg_only)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
