import os
import lorem

script_dir = os.path.dirname(os.path.realpath(__file__))
script_parent_dir = os.path.dirname(script_dir)
test_files_dir = os.path.join(script_parent_dir, 'test_files')

test_files = [
    'test_file_1.txt',
    'folder1/test_file_2.txt',
    'folder1/test_file_3.txt',
    'folder1/folder2/test_file_4.txt',
    'folder2/',
]

for test_file in test_files:
    test_file_path = os.path.join(test_files_dir, test_file)
    if not os.path.exists(os.path.dirname(test_file_path)):
        os.makedirs(os.path.dirname(test_file_path))
    if test_file.endswith('/'):
        continue
    with open(test_file_path, 'w') as f:
        f.write(lorem.paragraph())
