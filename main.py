import random
import time
import os

t0 = time.time()
print(os.getcwd())
answer = input("Did you update the cells? (y/n): ").lower()
if answer == 'n':
    print("Program terminated. Please update cells before continuing.")
    exit()
elif answer == 'y':
    # Program continues with existing code
    pass
else:
    print("Invalid input. Please enter 'y' or 'n'.")
    exit()
def repeat():
    a = random.randint(1, 4)
    b = random.randint(1, 6)
    c = random.randint(1, 8)
    d = random.randint(1, 10)
    e = random.randint(1, 12)
    f = random.randint(1, 20)

    if a == 4:
        if b == 6:
            if c == 8:
                if d == 10:
                    if e == 12:
                        if f == 20:
                            return True

mylist = []
for x in range(1):
    loops = 0
    while True:
        if repeat(): break
        loops += 1

print(loops)
t1 = time.time()
total = t1-t0
print(total)

