import itertools
import os
import time
from itertools import cycle

import numpy as np
from scipy.optimize import minimize
from sklearn import linear_model
from sklearn.preprocessing import PolynomialFeatures
from sklearn.cluster import DBSCAN
from operator import add
from optparse import OptionParser

from transfer_experiment import TransferExperiment

discarded_group_counter = 0


def parseArguments():
    parser = OptionParser()
    parser.add_option("-f", "--file",
                      action="store", type="string", dest="filename")
    parser.add_option("-p", "--paralelism", action="store", type="int",
                      dest="sample_parallelism", help="Parallelism level used in sample transfer")
    parser.add_option("-c", "--cc", "--concurrency", action="store", type="int",
                      dest="sample_concurrency", help="Concurrency level used in sample transfer")
    parser.add_option("-q", "--pipelining", "--ppq", action="store", type="int",
                      dest="sample_pipelining", help="Pipelining level used in sample transfer")
    parser.add_option("-t", "--throughput", action="store", type="float",
                      dest="sample_throughput", help="Transfer throughput obtained in sample transfer")
    return parser.parse_args()


def read_data_from_file(file_id):
    """"
     reads parameter value-throughput data from file
     file is structured to hold metadata followed by entries
     sg5-25M.csv 10
     entry1
     .
     entry10
     sg1G.csv 432
    """
    try:
        name, size = file_id.next().strip().split()  # skip first line
        data = np.genfromtxt(itertools.islice(file_id, int(size)), delimiter=' ')
        return data, name, size
    except:
        return None, None, None


def run_modelling(data):
    maximums = data.max(axis=0)
    min_training_score = 0.8
    min_test_score = 0.7
    for degree in range(4):
        polynomial_features = PolynomialFeatures(degree=degree)
        regression, train_score, test_score = run_regression(polynomial_features, data)
        optimal_point = find_optimal_point(polynomial_features, regression, maximums)
        optimal_point_thr = -1 * optimal_point.fun
        if optimal_point_thr < maximums[-1] * 3 and train_score > min_training_score and test_score > min_test_score:
            return regression, degree, optimal_point
    # print "Skipped1", estimated_thr, max_observed_throughput, train_score, test_score
    return None, None, None


def run_regression(poly, data):
    regression_model = linear_model.LinearRegression()

    np.random.shuffle(data)
    train_data, test_data = np.vsplit(data, [int(data.shape[0]*.8)])

    # Training score
    train_params = train_data[:, 0:3]
    train_thr = train_data[:, -1]
    train_params_ = poly.fit_transform(train_params)
    # preform the actual regression
    regression_model.fit(train_params_, train_thr)
    train_score = regression_model.score(train_params_, train_thr)

    # Test score
    test_params = test_data[:, 0:3]
    test_thr = test_data[:, -1]
    testing_params_ = poly.fit_transform(test_params)
    test_score = regression_model.score(testing_params_, test_thr)
    # print train_score, test_score

    return regression_model, train_score, test_score


def find_optimal_point(poly, regression, maximums):
    func = convert_to_equation(poly, regression)
    bounds = (1, maximums[0]), (1, maximums[1]), (0, maximums[2])
    guess = [1, 1, 0]
    return minimize(func, guess, bounds=bounds, method='L-BFGS-B')


# Converts polynomial equation parameters found by PolynomialFeatures to an lambda equation
def convert_to_equation(poly, clf):
    features = []
    for i in range(3):
        features.append('x[' + str(i) + ']')
    coefficients = cycle(clf.coef_)
    # skip first coefficient as it is always 0, not sure why
    coefficients.next()
    equation = ''
    for entry in poly.powers_:
        new_feature = []
        for feat, coef in zip(features, entry):
            if coef > 0:
                new_feature.append(feat+'**'+str(coef))
        if not new_feature:
            equation = str(clf.intercept_)
        else:
            equation += ' + '
            equation += '*'.join(new_feature)
            equation += '*' + str(coefficients.next())
    return lambda x: eval('-1 * (' + equation + ')')


def main():
    (options, args) = parseArguments()
    chunk_name = options.filename
    sample_transfer_params = np.array([options.sample_concurrency, options.sample_parallelism,
                                            options.sample_pipelining])
    sample_transfer_throughput = options.sample_throughput
    file_name = os.path.join(os.path.dirname(__file__), '../../../', 'out', chunk_name)
    discarded_data_counter = 0
    all_experiments = []
    fin = open(file_name, 'r')
    data, name, size = read_data_from_file(fin)
    while data is not None:
        regression, degree, optimal_point = run_modelling(data)
        if regression is None:
            #print name, size
            discarded_data_counter += 1
        else:
            all_experiments.append(TransferExperiment(name, size, regression, degree, optimal_point))
        data, name, size = read_data_from_file(fin)
    #print "Skipped:", discarded_data_counter,  "/", (len(all_experiments) + discarded_data_counter)
    fin.close()


    for experiment in all_experiments:
        poly = PolynomialFeatures(degree=experiment.poly_degree)
        estimated_troughput = experiment.regression.predict(poly.fit_transform(sample_transfer_params.reshape(1, -1)))
        experiment.set_closeness(abs(estimated_troughput-sample_transfer_throughput))
        # print experiment.name, estimated_troughput, " diff:", (estimated_troughput-sample_transfer_throughput)

    all_experiments.sort(key=lambda x: x.closeness, reverse=True)
    attrs = [experiment.closeness for experiment in all_experiments]
    db = DBSCAN(eps=50, min_samples=1).fit(attrs)
    labels = db.labels_

    for experiment in all_experiments:
        experiment.run_parameter_relaxation()

    total_weight = 0
    total_thr = 0
    total_params = [0, 0, 0]

    for experiment, label in zip(all_experiments, labels):
        if label == -1:
            continue
        weight = 2 ** label
        total_weight += weight
        weighted_params = [param * weight for param in experiment.relaxed_params]
        #print weighted_params, total_params
        total_params = map(add, total_params, weighted_params)
        total_thr += weight * experiment.optimal_point.fun

    final_params = total_params/total_weight
    final_throughput = -1 * total_thr/total_weight
    print final_params, final_throughput
    #return (total_params/total_weight), (total_thr/total_weight)

if __name__ == "__main__":
    start_time = time.time()
    main()
    #print("--- %s seconds ---" % (time.time() - start_time))
