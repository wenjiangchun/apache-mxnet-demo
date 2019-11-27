import random

for i in range(20):
    sn = '(' + str(i + 1) + ')'
    #print(sn)
    a = random.randint(1, 1000)
    b = random.randint(1, 1000)
    
    #print(sn + ' ' + str(a) + ' + ' + str(b) + '=' )

    if (a < b ):
        print(sn + ' ' + str(b) + ' - ' + str(a) + '=' )
    else:
        print(sn + ' ' + str(a) + ' - ' + str(b) + '=' )
    
    #print(sn + ' ' + str(a) + ' + ' + str(b) + '=' )

    print('  ')
    print('  ')
    print('  ')
