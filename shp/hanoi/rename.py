
import os

def rename_files(directory):
    for filename in os.listdir(directory):
        if "hn3" in filename:
            new_filename = filename.replace("hn3", "hanoi")
            old_path = os.path.join(directory, filename)
            new_path = os.path.join(directory, new_filename)
            os.rename(old_path, new_path)
            print(f'Renamed: {filename} -> {new_filename}')

# Example usage
directory = r'D:\\Project\\SHP\\hanoi\\'  # Change this to your directory path
rename_files(directory)
