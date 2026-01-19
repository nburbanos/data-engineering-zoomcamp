import sys

#Arguments get printed when you executed python command to run script
print('arguments', sys.argv)

month = int(sys.argv[1])
print(f'Hello pipeline, month={month}')