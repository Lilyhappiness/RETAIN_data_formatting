import pandas as pd
import argparse
from sklearn.utils import shuffle


def shuffle_labels(ARGS):
	"""
	randomly shuffle the order of the target labels 
	(for RETAIN model testing purposes)
	"""
	labels = pd.read_pickle(ARGS.path_target)
	shuffled_labels = shuffle(labels)
	shuffled_labels.to_pickle(ARGS.path_new_target)

def parse_arguments(parser):
    """Read user arguments"""
    parser.add_argument('--path_target', type=str, default='data/target_test.pkl',
                        help='Path to evaluation target file')
    parser.add_argument('--path_new_target', type=str, default='data/target_test_shuffled.pkl',
                        help='Path to shuffled/randomized output evaluation target file')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ARGS = parse_arguments(PARSER)
    print (ARGS)
    shuffle_labels(ARGS)