from sklearn.preprocessing import PolynomialFeatures
import numpy as np

class TransferExperiment(object):

    def __init__(self, name, count, regression=None, poly_degree=None, optimal_point=None):
        """

        :param name: str
        :param count:
        :param regression:
        :param degree:
        :param optimal_point:
        """
        self.name = name
        self.count = count
        self.regression = regression
        self.optimal_point = optimal_point
        self.poly_degree = poly_degree
        self.closeness = None
        self.relaxed_params = None
        self.relaxed_throughput = None

    def set_regression(self, regression):
        """

        :param regression:
        :return:
        """
        self.regression = regression

    def set_optimal_point_result (self, optimal_point):
        self.optimal_point = optimal_point

    def set_closeness (self, closeness):
        self.closeness = closeness

    def run_parameter_relaxation(self):

        poly = PolynomialFeatures(degree=self.poly_degree)
        optimal_cc = int(self.optimal_point.x[0])
        optimal_throughput_revised = -1 * self.optimal_point.fun
        #print "Optimal CC ", optimal_cc, "Thr:", optimal_throughput_revised
        relaxed_cc = optimal_cc
        for relaxed_cc in range(optimal_cc-1, 0, -1):
            new_params = np.array([relaxed_cc, self.optimal_point.x[1], self.optimal_point.x[2]])
            throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
            if throughput < 0.8 * optimal_throughput_revised:
                relaxed_cc += 1
                new_params = np.array([relaxed_cc, self.optimal_point.x[1], self.optimal_point.x[2]])
                throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
                optimal_throughput_revised = throughput
                break

        optimal_p = int(self.optimal_point.x[1])
        #print "Optimal p ", optimal_p, "Thr:", optimal_throughput_revised
        relaxed_p = optimal_p
        for relaxed_p in range(optimal_p-1, 0, -1):
            new_params = np.array([relaxed_cc, relaxed_p, self.optimal_point.x[2]])
            throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
            if throughput < 0.9 * optimal_throughput_revised:
                relaxed_p += 1
                new_params = np.array([relaxed_cc, relaxed_p, self.optimal_point.x[2]])
                throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
                optimal_throughput_revised = throughput
                break

        estimated_optimal_ppq = int(self.optimal_point.x[2])
        relaxed_ppq = estimated_optimal_ppq
        for relaxed_ppq in range(estimated_optimal_ppq-1, -1, -1):
            new_params = np.array([relaxed_cc, relaxed_p, relaxed_ppq])
            throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
            if throughput < 0.99 * optimal_throughput_revised:
                relaxed_ppq += 1
                new_params = np.array([relaxed_cc, relaxed_p, relaxed_ppq])
                throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
                optimal_throughput_revised = throughput
                break
        self.relaxed_params = [relaxed_cc, relaxed_p, relaxed_ppq]
        self.relaxed_throughput = optimal_throughput_revised
