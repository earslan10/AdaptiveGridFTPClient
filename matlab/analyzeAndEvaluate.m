function [trainingPcp,estimatedThroughput, accuracy] = analyzeAndEvaluate(fileName, targetThroughput, sampleValues )
    [trainingPcp,estimatedThroughput] = main(strcat(fileName, '_training.txt'), targetThroughput, sampleValues,[],0 );
    [~, accuracy] = main(strcat(fileName, '_testing.txt'), targetThroughput, sampleValues, trainingPcp, estimatedThroughput );
end