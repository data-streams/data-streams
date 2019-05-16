import matplotlib.pyplot as plt
import numpy as np

def draw_graph():
    inf = open("tmp.txt", "r")
    rus = list(map(float, inf.readline().split(',')))
    put = list(map(float, inf.readline().split(',')))
    ukr = list(map(float, inf.readline().split(',')))
    inf.close()

    plt.plot(rus)
    plt.plot(put)
    plt.plot(ukr)
    plt.legend(["россия", "путин", "украина"])
    plt.title("политика")
    plt.xlabel("n_docs")
    plt.ylabel("weight")
    plt.show()

if __name__ == '__main__':
    draw_graph()