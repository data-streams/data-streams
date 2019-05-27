import matplotlib.pyplot as plt
import numpy as np
import scipy as sp
import scipy.stats

def mean_confidence_interval(data, confidence=0.95):
    a = 1.0*np.array(data)
    n = len(a)
    m, se = np.mean(a), scipy.stats.sem(a)
    h = se * sp.stats.t._ppf((1+confidence)/2., n-1)
    return m, m-h, m+h

def draw_graph(axvs, topic):
    inf = open("tmp.txt", "r")
    legend = []
    for line in inf.readlines():
        tmp = line.split(',')
        plt.plot(list(map(float, tmp[1:])))
        legend.append(tmp[0])

    inf.close()
    for axv in axvs:
        plt.axvline(axv, color='red')
    plt.legend(legend)
    plt.title(topic)
    plt.xlabel("n_docs")
    plt.ylabel("weight")
    plt.show()

def draw_hists():
    inf = open("tmp_hist.txt", "r")
    topics = inf.readline().split(',')

    fig = plt.figure()
    bar_width = 0.8
    x = np.arange(len(topics))

    counts = np.array(list(map(float, inf.readline().split(' '))))
    counts /= np.sum(counts)
    plt.bar(x - bar_width / 2, counts, color='r', width=bar_width / 3)


    counts = np.array(list(map(float, inf.readline().split(' '))))
    counts /= np.sum(counts)
    plt.bar(x - bar_width / 6, counts, color='b', width=bar_width / 3)

    counts = np.array(list(map(float, inf.readline().split(' '))))
    counts /= np.sum(counts)
    plt.bar(x + bar_width / 6, counts, color='g', width=bar_width / 3)

    fig.set_size_inches(18.5, 8.5, forward=True)
    plt.xticks(x, topics, rotation='vertical')
    plt.legend(['offline test=4000, warmup=8000', 'online test=4000, train=4000, warmup=4000', 'correct'])
    plt.ylabel('docs percentage')
    plt.show()

def draw_topics():
    inf = open("tmp_hist.txt", "r")
    topics = inf.readline().split(',')
    online = np.array(list(map(float, inf.readline().split(' '))))
    offline = np.array(list(map(float, inf.readline().split(' '))))
    correct = np.array(list(map(float, inf.readline().split(' '))))

    interesting = ['футбол', 'госэкономика', 'люди', 'музыка', 'кино', 'наука']

    fig, axs = plt.subplots(1, len(interesting), sharey=True)

    for i in range(len(interesting)):
        axs[i].set_title(interesting[i])
        j = topics.index(interesting[i])

        bar_width = 0.8
        x = np.array([0])

        axs[i].bar(x - bar_width / 2, [online[j] / np.sum(online)], color='r', width=bar_width / 3)
        axs[i].bar(x - bar_width / 6, [offline[j] / np.sum(offline)], color='b', width=bar_width / 3)
        axs[i].bar(x + bar_width / 6, [correct[j] / np.sum(correct)], color='g', width=bar_width / 3)

        axs[i].set_xticks(x)

    fig.set_size_inches(18.5, 8.5, forward=True)

    plt.legend(['offline test=4000, warmup=8000', 'online test=4000, train=4000, warmup=4000', 'correct'])
    plt.ylabel('docs percentage')
    plt.show()

if __name__ == '__main__':
    draw_hists()