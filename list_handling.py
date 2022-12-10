a = [2, 24, 10, 1, 35, 2, 70]
print(a)
a.remove(35)  # remove 35 from list
print(a)
z = a.pop(1)  # remove element at 1 index we can catch also
print(a)
print(z)
a.append(88)  # add 88 at last
print(a)
a.insert(3, 100)  # add 100 at 3 index
print(a)
b = a.index(10)  # get the index for 10
print(b)
print(a)
if 99 in a:
    c = a.index(99)
    print(c)
print(min(a),max(a),len(a),sum(a))
