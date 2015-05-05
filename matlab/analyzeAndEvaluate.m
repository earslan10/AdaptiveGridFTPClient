    function [final,val] = analyzeAndEvaluate(folderID, targetThroughput, trialNumber, sampleValues, output_dir )
    testCount = round(trialNumber*0.7);
    trainingCount = trialNumber - testCount;
    testTrials = linspace(0,testCount-1,testCount);
    trainingTrials =linspace(testCount,testCount+trainingCount-1,trainingCount);
    [pcp,estimatedThroughput] = main(folderID, targetThroughput, testTrials, sampleValues, output_dir,[],0 );
    main(folderID, targetThroughput, trainingTrials, sampleValues, output_dir, pcp, estimatedThroughput );
end

