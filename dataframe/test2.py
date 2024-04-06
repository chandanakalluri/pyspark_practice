
s = "InternshipTrainingProgram"
even_list = []
odd_list = []
for i in range(0,len(s)):
    if i%2==0:
        even_list.append(s[i])
    else:
        odd_list.append(s[i])
print(s)
print(even_list)
print(odd_list)

# out = "sihT si doog yad"
s = "This is good day"
print(s)
str=s.split()
str1=""
for i in str:
    for char in i:
        str1 = char + str1
    print(str1,end=" ")
    str1=""






