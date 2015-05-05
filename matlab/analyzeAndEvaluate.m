function [trainingPcp,estimatedThroughput, accuracy] = analyzeAndEvaluate(folderID, targetThroughput, trialNumber, sampleValues, output_dir )
    testCount = round(trialNumber*0.7);
    trainingCount = trialNumber - testCount;
    testTrials = linspace(0,testCount-1,testCount);
    trainingTrials =linspace(testCount,testCount+trainingCount-1,trainingCount);
    [trainingPcp,estimatedThroughput] = main(folderID, targetThroughput, testTrials, sampleValues, output_dir,[],0 );
    [~, accuracy] = main(folderID, targetThroughput, trainingTrials, sampleValues, output_dir, trainingPcp, estimatedThroughput );
end

