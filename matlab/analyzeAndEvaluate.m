function [trainingPcp,estimatedThroughput, accuracy] = analyzeAndEvaluate(folderID, targetThroughput, trialNumber, sampleValues, output_dir )
    trainingCount = round(trialNumber*0.7);
    testCount = trialNumber - trainingCount;
    allTrials = linspace(0,trialNumber-1,trialNumber);
    allTrials = allTrials(randperm(length(allTrials)));
    testTrials = allTrials(1:trainingCount);
    trainingTrials =allTrials(trainingCount+1 : end);
    [trainingPcp,estimatedThroughput] = main(folderID, targetThroughput, testTrials, sampleValues, output_dir,[],0 );
    [~, accuracy] = main(folderID, targetThroughput, trainingTrials, sampleValues, output_dir, trainingPcp, estimatedThroughput );
end

