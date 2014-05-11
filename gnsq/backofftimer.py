from random import randint


class BackoffTimer(object):
    def __init__(self, ratio=1, max_interval=None, min_interval=None):
        self.c     = 0
        self.ratio = ratio

        self.max_interval = max_interval
        self.min_interval = min_interval

    def reset(self):
        self.c = 0
        return self

    def success(self):
        self.c = max(self.c - 1, 0)
        return self

    def failure(self):
        self.c += 1
        return self

    def get_interval(self):
        k = pow(2, self.c) - 1
        interval = randint(0, k) * self.ratio

        if self.max_interval is not None:
            interval = min(interval, self.max_interval)

        if self.min_interval is not None:
            interval = max(interval, self.min_interval)

        return interval
