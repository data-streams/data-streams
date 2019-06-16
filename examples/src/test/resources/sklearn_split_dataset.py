# coding=utf-8
import numpy as np
import pandas as pd
import sys
import time
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from scipy.sparse import vstack


def put(X, y, file):
    ouf = open(file, "w+")
    ouf.write(str(len(X)) + ' ' + str(X[0].shape[1]) + '\n')
    for (x, y1) in zip(X, y):
        nz = x.nonzero()[1]
        row = x.toarray()[0]
        ouf.write(y1)
        for j in nz:
            ouf.write(',' + str(j) + ',' + str(row[j]))
        ouf.write('\n')
    ouf.close()

def putWindow(X, y, isTrain, file):
    ouf = open(file, "w+")
    ouf.write(str(len(X)) + ' ' + str(X[0].shape[1]) + '\n')
    for (x, y1) in zip(X, y):
        nz = x.nonzero()[1]
        row = x.toarray()[0]
        ouf.write(y1)
        for j in nz:
            ouf.write(',' + str(j) + ',' + str(row[j]))
        ouf.write('\n')
    ouf.write(','.join(isTrain) + '\n')
    ouf.close()

def calcWindowIdf(X, windowSizeDays, lam):
    X = X.astype(float)
    size, features = X.shape

    windowSize = windowSizeDays * 200

    idf = np.array([0] * features, dtype=float)
    ans = []
    cur = 0
    index = 0
    for tf in X:
        # Go to the next window if it's time
        if index - cur >= windowSize:
            idf *= lam
            cur = index

        # Update cur_idf
        for j in tf.nonzero()[1]:
            idf[j] += 1

        tf = tf / tf.sum()
        tf = tf.multiply(np.log(windowSize / idf))
        #tf.data = tf.data + 1
        tf /= np.sqrt(tf.multiply(tf).sum())

        ans.append(tf)

        index += 1

    return ans

def calcStreamingIdf(X):
    X = X.astype(float)
    size, features = X.shape

    idf = np.array([0.0001] * features, dtype=float)
    ans = []
    cur = 0
    index = 0
    for tf in X:
        cur += 1
        # Update cur_idf
        for j in tf.nonzero()[1]:
            idf[j] += 1

        tf = tf / tf.sum()
        tf = tf.multiply(np.log(cur / idf))
        #tf.data = tf.data + 1
        tf /= np.sqrt(tf.multiply(tf).sum())

        ans.append(tf)

        index += 1

    return ans


def main(argv):
    if len(argv) < 7:
        print("Usage {} trainSize testSize warmUp offset seed splitStrategy [windowSizeDays] [dampingFactor]".format(argv[0]))
        exit(0)
    trainSize = int(argv[1])
    testSize = int(argv[2])
    warmUp = int(argv[3])
    offset = int(argv[4])
    seed = int(argv[5])
    splitStrategy = argv[6]
    windowSize = 0
    lam = 0
    if (splitStrategy == 'window' or splitStrategy == 'window_test_train'):
        if (len(argv) < 8):
            print("{}: windowSizeDays and dampingFactor are necessary for window splitStrategy".format(argv[0]))
            exit(0)
        windowSize = float(argv[7])
        lam = float(argv[8])

    df = pd.read_csv('news_lenta.csv', nrows=(trainSize + testSize + warmUp + offset)*2)
    df = df.dropna()
    df = df[df['tags'] != 'Все']
    df = df[df['tags'] != '']
    df = df[offset:]
    df = df[:warmUp + trainSize + testSize]
    df = df[::-1]

    X = df['text']
    y = df['tags']

    if (splitStrategy == 'complete_streaming'):
        vectorizer = CountVectorizer()
        X = vectorizer.fit_transform(X)

        voc = vectorizer.vocabulary_

        ouf = open('cnt_vectorizer', 'w')
        for key, val in voc.items():
            ouf.write(str(key) + ' ' + str(val) + '\n')
        ouf.close()

        transformer = TfidfTransformer(sublinear_tf=True)
        # X = list(transformer.fit_transform(X))
        X = calcStreamingIdf(X)

        size = trainSize + testSize + warmUp
        _, test = train_test_split(range(warmUp, size), test_size=testSize, random_state=seed)
        isTrain = list(map(str, map(int, [i not in test for i in range(size)])))

        putWindow(X, y, isTrain, 'tmp_train')

        exit(0)

    if (splitStrategy == 'window'):
        vectorizer = CountVectorizer()
        X = vectorizer.fit_transform(X)

        voc = vectorizer.vocabulary_

        ouf = open('cnt_vectorizer', 'w')
        for key, val in voc.items():
            ouf.write(str(key) + ' ' + str(val) + '\n')
        ouf.close()

        print("Step inside calcWindowIdf")
        tm = time.monotonic()
        X = calcWindowIdf(X, windowSize, lam)
        print("Execution time is: ", time.monotonic() - tm)

        size = trainSize + testSize + warmUp
        _, test = train_test_split(range(warmUp, size), test_size=testSize, random_state=seed)
        isTrain = list(map(str, map(int, [i not in test for i in range(size)])))

        putWindow(X, y, isTrain, 'tmp_train')

        exit(0)

    if (splitStrategy == 'window_test_train'):
        vectorizer = CountVectorizer()
        X = vectorizer.fit_transform(X)

        voc = vectorizer.vocabulary_

        ouf = open('cnt_vectorizer', 'w')
        for key, val in voc.items():
            ouf.write(str(key) + ' ' + str(val) + '\n')
        ouf.close()

        print("Step inside calcWindowIdf")
        tm = time.monotonic()
        X = calcWindowIdf(X, windowSize, lam)
        print("Execution time is: ", time.monotonic() - tm)

        X1 = list(X[warmUp:])
        y1 = list(y[warmUp:])
        X = list(X[:warmUp])
        y = list(y[:warmUp])

        X1 = list(enumerate(X1))
        y1 = list(enumerate(y1))

        _, X_test, _, y_test = train_test_split(X1, y1, test_size=testSize, random_state=seed)

        X_test = [t[1] for t in sorted(X_test, key = lambda t: t[0])]
        y_test = [t[1] for t in sorted(y_test, key = lambda t: t[0])]

        X_train = X
        y_train = y

        put(X_train, y_train, "tmp_train")
        put(X_test, y_test, "tmp_test")

        exit(0)

    if splitStrategy == 'no_shuffle_complete':
        vectorizer = CountVectorizer()
        X = vectorizer.fit_transform(X)

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=testSize, random_state=seed, shuffle=False)

        transformer = TfidfTransformer(sublinear_tf=True)

        X_train = list(transformer.fit_transform(X_train))
        X_test = list(transformer.transform(vstack(X_test)))

        put(X_train, y_train, "tmp_train")
        put(X_test, y_test, "tmp_test")

        exit(0)

    if splitStrategy == 'warmup_test_complete':
        vectorizer = CountVectorizer()
        X = vectorizer.fit_transform(X)

        X_train = X[:warmUp]
        y_train = y[:warmUp]
        X = list(enumerate(X[warmUp:]))
        y = list(enumerate(y[warmUp:]))
        _, X_test, _, y_test = train_test_split(X, y, test_size=testSize, random_state=seed)

        X_test = [t[1] for t in sorted(X_test, key = lambda t: t[0])]
        y_test = [t[1] for t in sorted(y_test, key = lambda t: t[0])]

        transformer = TfidfTransformer(sublinear_tf=True)

        X_train = list(transformer.fit_transform(X_train))
        X_test = list(transformer.transform(vstack(X_test)))

        put(X_train, y_train, "tmp_train")
        put(X_test, y_test, "tmp_test")

        exit(0)

    processing = Pipeline([
        ('vect', CountVectorizer()),
        ('tfidf', TfidfTransformer(sublinear_tf=True))])

    X = processing.fit_transform(X)

    X1 = list(X[warmUp:])
    y1 = list(y[warmUp:])
    X = list(X[:warmUp])
    y = list(y[:warmUp])

    if (splitStrategy == 'ordered_complete'):
        X1 = list(enumerate(X1))
        y1 = list(enumerate(y1))

    X_train, X_test, y_train, y_test = train_test_split(X1, y1, test_size=testSize, random_state=seed)

    if (splitStrategy == 'ordered_complete'):
        X_train = [t[1] for t in sorted(X_train, key = lambda t: t[0])]
        y_train = [t[1] for t in sorted(y_train, key = lambda t: t[0])]

        X_test = [t[1] for t in sorted(X_test, key = lambda t: t[0])]
        y_test = [t[1] for t in sorted(y_test, key = lambda t: t[0])]

    X_train = X + X_train
    y_train = y + y_train

    put(X_train, y_train, "tmp_train")
    put(X_test, y_test, "tmp_test")

if __name__ == "__main__":
    main(sys.argv)