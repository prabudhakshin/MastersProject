from pylab import *

def ll(alist):
  return alist[:600]

f1 = open("com_a.txt", "r")
com_a = [val for val in f1]

f1 = open("com_all.txt", "r")
com_all = [val for val in f1]

f1 = open("all_a.txt", "r")
all_a = [val for val in f1]

f1 = open("all_all.txt", "r")
all_all = [val for val in f1]

figure()
#subplot(221)
p1, = plot(ll(com_a))
p2, = plot(ll(com_all))
p3, = plot(ll(all_a))
p4, = plot(ll(all_all))
grid()
legend([p4,p3,p2,p1],["mixed TLD/ALL", "mixed TLD/A", ".com/ALL", ".com/A"], loc=4, prop={'size':17})

#subplot(222)
#plot(com_all)
#title(".com domain, qtype ALL")
#
#subplot(223)
#plot(all_a)
#title("Mixed TLD, qtype A")
#
#subplot(224)
#plot(all_all)
#title("Mixed TLD, qtype ALL")
#
xlabel("#Domains queried", fontsize=17)
ylabel("#Buckets Searched", fontsize=17)
show()
