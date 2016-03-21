function [trainingPcp,estimatedThroughput, accuracy] = analyzeAndEvaluate(fileName, targetThroughput, sampleValues, bandwidth )
    [trainingPcp,estimatedThroughput] = main(strcat(fileName, '_training.txt'), targetThroughput, sampleValues,[],0, bandwidth);
    [~, accuracy] = main(strcat(fileName, '_testing.txt'), targetThroughput, sampleValues, trainingPcp, estimatedThroughput, bandwidth );
end