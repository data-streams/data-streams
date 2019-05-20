import matplotlib.pyplot as plt
import numpy as np

def draw_graph():
    inf = open("tmp.txt", "r")
    legend = []
    for line in inf.readlines():
        tmp = line.split(',')
        plt.plot(list(map(float, tmp[1:])))
        legend.append(tmp[0])

    inf.close()

    plt.legend(legend)
    plt.title("политика")
    plt.xlabel("n_docs")
    plt.ylabel("weight")
    plt.show()

if __name__ == '__main__':
    draw_graph()