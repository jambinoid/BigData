import argparse
import random
import struct

from tqdm import tqdm


def generate_file(output_path: str):
    with open(output_path, 'wb') as f:
        for i in tqdm(range(262144 * 2048)):
            num = random.randint(0, 4294967295)
            f.write(struct.pack('>I', num))
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate binary file of 32 bit random unsigned integers.')
    parser.add_argument('--output_path',
                        help='Path to output file.',
                        type=str)
    parser.add_argument('--seed',
                        help='Pseudorandom seed to reproduce experiment.',
                        type=int)

    args = parser.parse_args()
    
    if args.seed:
        random.seed(args.seed)
        
    generate_file(args.output_path)