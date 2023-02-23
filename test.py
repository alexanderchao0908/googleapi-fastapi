a = "AB"
b = "B"


def cmp_str(a, b):
    if len(a) < len(b):
        res = 1
    if len(a) > len(b):
        res = -1
    if len(a) == len(b):
        if a < b:
            res = 1
        elif a > b:
            res = -1
        else:
            res = 0

    return res


print(cmp_str(a, b))
