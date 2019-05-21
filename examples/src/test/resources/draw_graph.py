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

    plt.axvline(5000, color='red')
    plt.legend(legend)
    plt.title("политика")
    plt.xlabel("n_docs")
    plt.ylabel("weight")
    plt.show()

def draw_hists():
    inf = open("tmp_hist.txt", "r")
    topics = inf.readline().split(',')
    counts = np.array(list(map(float, inf.readline().split(' '))))
    counts /= np.sum(counts)
    x = np.arange(len(counts))

    fig = plt.figure()

    bar_width = 0.8
    plt.bar(x - bar_width / 2, counts, color='b', width=bar_width / 3)


    counts = np.array(list(map(float, inf.readline().split(' '))))
    counts /= np.sum(counts)


    plt.bar(x - bar_width / 6, counts, color='r', width=bar_width / 3)

    counts = np.array(list(map(float, inf.readline().split(' '))))
    counts /= np.sum(counts)
    plt.bar(x + bar_width / 6, counts, color='g', width=bar_width / 3)

    fig.set_size_inches(18.5, 10.5, forward=True)
    plt.xticks(x - bar_width / 2, topics, rotation='vertical')
    plt.legend(['online', 'offline', 'correct'])
    plt.ylabel('docs percentage')
    plt.show()

if __name__ == '__main__':
    draw_hists()