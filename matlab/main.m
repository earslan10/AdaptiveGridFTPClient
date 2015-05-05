%clear; clc; format compact; close all

function [final,val] = main(folderID, targetThroughput, trialNumber, sampleValues, output_dir, testPcp, testThroughput )
    
    maxEvaluatedDegree = 3;
    targetThroughput = targetThroughput/(1000*1000);
	bestTrial = 0;
    bestTrialDegree = 1;
	minEstimationError = realmax;
    estimations = zeros(size(trialNumber,2),1);
    
    %bestEquationOfTrials = zeros(trialNumber);
    options = optimset('Display','off');
    
    reasonableTrials = 0;
	folderName = strcat(output_dir,'/chunk_',num2str(folderID),'/trial-');
	%for trial = 1:1
    for trial = trialNumber
        disp(['Trial# ', num2str(trial)]);
	    %fprintf(fid, 'Trial# %d\n', (trial+1));
	    
	    filename = strcat(folderName, num2str(trial),'.txt');
	    R2 = 0;
        maximumRsquare = 0;
	    %evaluate various levels of equation degree 
        bestDegree = 1;
        %currentRMSE = realmax;
	    for degree= 1:maxEvaluatedDegree
            
            [equation, R2, RMSE, maxVals] = findEquation(filename, degree);
           
            f = inline(equation);
            %objectiveFunction = inline([' -1 *(' char(equation) ')']);
            %[t,val] = fmincon(objectiveFunction,[1,1,0],[],[],[],[],[1,1,0], maxValues , [], options);
            estimation = f(sampleValues);
            %f([32,8,0])
            disp(['Degree# ', num2str(degree), ' R2:', num2str(R2),' estimation:', num2str(estimation)]);
             %choose the one with highest R2
             if R2 > maximumRsquare && estimation < 10^4 && estimation > 0
                 localBestMatchEquation  = equation;
                 maximumRsquare = R2;
                 bestDegree = degree;
             else
                 break;
             end
             
        end
	    f = inline(localBestMatchEquation);
        %disp(strcat('Trial:',num2str(trial) , ' best-equation:', localBestMatchEquation));
        bestEquationOfTrials{trial+1} = localBestMatchEquation;
        maxParamValues{trial+1} = maxVals;
        %sampleValues
        %localBestMatchEquation
	    estimation = f(sampleValues);           
        if abs(targetThroughput - estimation) <  10^4 
            estimations(reasonableTrials+1) = abs(targetThroughput - estimation);
            reasonableTrials = reasonableTrials +1;
            if abs(targetThroughput - estimation) < minEstimationError
                 bestMatchEquation = f;
                 minEstimationError = abs(targetThroughput - estimation);
                 bestTrial = trial;
                 bestTrialDegree = bestDegree;
                 
            end
            disp(strcat('Trial:',num2str(bestTrial) , ' estimation:', num2str(estimation),...
                     ' error:', num2str(minEstimationError)));
        else
            disp(strcat('Skipping:',num2str(trial) , ' estimation:', num2str(estimation),...
                     ' error:', num2str(abs(targetThroughput - estimation))));
        end
    end
    
   %return
    
    estimations
    %Find optimal throughput and corresponding pcp values for the best
    %matching equation
	filename = strcat(folderName, num2str(bestTrial),'.txt');
    
    [equation, R2, RMSE] = findEquation(filename, bestTrialDegree);
    objectiveFunction = inline([' -1 *(' char(equation) ')']);
	[t,val] = fmincon(objectiveFunction,[1,1,0],[],[],[],[],[1,1,0], maxParamValues{bestTrial+1} , [], options);
    disp(strcat('Best trial:', num2str(bestTrial),' degree:', num2str(bestTrialDegree), ' error:',...
        num2str(minEstimationError),' Peak throughput point:', num2str(val), ' values: ', num2str(t)));
    
    stdev = std(estimations);
    avg = mean(estimations);
    disp(strcat('Min Error:', num2str(minEstimationError), 'Mean:', num2str(avg),' Stdev:', num2str(stdev)));
    %minEstimationError 
    
    disp(strcat('Trials error less than:', num2str(minEstimationError + stdev)));
    
    %Calculate weighted average for pcp
    cc = 0;
    p = 0;
    ppq = 0;
    totalWeight = 0;
    totalThrouhput = 0;
    testThroughput = 0;
    %for trial = 1:1
    for trial = trialNumber
        localBestMatchEquation = bestEquationOfTrials{trial+1};
        maxParamValue = maxParamValues{trial+1};
        f = inline(localBestMatchEquation);
	    estimation = f(sampleValues);
        %if abs(targetThroughput - estimation) <  10^4 && abs(targetThroughput - estimation) < avg
        if abs(targetThroughput - estimation) <  10^4
                 objectiveFunction = inline([' -1 *(' char(localBestMatchEquation) ')']);
                 [t,val] = fmincon(objectiveFunction,[1,1,0],[],[],[],[],sampleValues,maxParamValue,[], options);
                 
                 if -1*val > 10^4
                      disp(strcat('Skipping:',num2str(trial) , ' estimation:', num2str(val)));
                      continue;
                 end
                 
                 weight = targetThroughput / (targetThroughput + abs(estimation- targetThroughput));
                 disp(strcat('Final Trial#',num2str(trial) ,' estimation:'...
                     ,num2str(estimation),' error:', num2str(abs(estimation - targetThroughput)), ...
                     ' weight:',num2str(weight), ' Val:', num2str(val)));
                 disp(strcat('Fmincon cc:',num2str(t(1)) ,' p:', num2str(round(t(2))), ...
                     ' ppq:', num2str(round(t(3))) , ' value:',num2str(-1*val)));
                 
                 thrEstimation = -1 * val;
                 newEstimation = thrEstimation;
                 
                 subOptimalPPQ = round(t(3));
                 if subOptimalPPQ >= 1
                     for subOptimalPPQ = 0 :1 :round(t(3))
                         newEstimation = f([t(1),t(2),subOptimalPPQ]);
                         %disp(strcat('Adjusted PPQ:',num2str(subOptimalPPQ) ,' estimation:',num2str(newEstimation)));
                         if newEstimation > thrEstimation * 0.9
                             disp(strcat('Adjusted PPQ:',num2str(subOptimalPPQ) ,' estimation:',num2str(newEstimation)));
                             break;
                         end
                     end
                 end
                 
                 thrEstimation = newEstimation;
                 subOptimalP = round(t(2));
                 if subOptimalP >= 1 
                     for subOptimalP = 1:round(t(2))
                         newEstimation = f([t(1),subOptimalP,subOptimalPPQ]);
                         %disp(strcat('CC:',num2str(subOptimalCC) ,' estimation:',num2str(newEstimation)));
                         if newEstimation > thrEstimation  * 0.9
                             disp(strcat('Adjusted P:',num2str(subOptimalP) ,' estimation:',num2str(newEstimation)));
                             break;
                         end
                     end
                 end
                 
                 thrEstimation = newEstimation;
                 
                 for subOptimalCC = 1:t(1)
                     newEstimation = f([subOptimalCC,subOptimalP,subOptimalPPQ]);
                     %disp(strcat('CC:',num2str(subOptimalCC) ,' estimation:',num2str(newEstimation)));
                     if newEstimation > thrEstimation * 0.9
                         disp(strcat('Adjusted CC:',num2str(subOptimalCC) ,' estimation:',num2str(newEstimation)));
                         break;
                     end
          
                 end
                 
                 cc = cc + subOptimalCC * weight;
                 %p = p + t(2) * weight;
                 p = p + subOptimalP * weight;
                 %ppq = ppq + t(3) * weight;
                 ppq = ppq + subOptimalPPQ * weight;
                 
                 totalThrouhput = totalThrouhput + f([subOptimalCC,subOptimalP,subOptimalPPQ]) * weight;
                 
                 totalWeight = totalWeight + weight;
                 
                 disp(strcat('All Adjusted cc:',num2str(subOptimalCC), ' p:',...
                     num2str(subOptimalP), ' ppq:',num2str(subOptimalPPQ)));
                   if isempty(testPcp) == 0
                       f(testPcp)
                       testThroughput = testThroughput +  weight *  f(testPcp);
                   end
                 
            %end
        end
    end

    cc = round( cc / totalWeight);
    p = round( p / totalWeight);
    ppq = round( ppq/ totalWeight);

    final(1) = cc;
    final(2) = p;
    final(3) = ppq;
    val = totalThrouhput/ totalWeight;
    disp(strcat('Optimal cc:',num2str(cc) ,' p:', num2str(p), ' ppq:', num2str(ppq),...
                 ' total Weight:', num2str(totalWeight), ' estimated thr:', num2str(val)));
             
    if isempty(testPcp) == 0
        testThroughput = testThroughput/totalWeight;
        val
        testThroughput
        accuracy = 100 - (abs(val-testThroughput)/val) * 100;
        disp(strcat('Test throughput:',num2str(testThroughput) ,' accuracy:', num2str(accuracy), '%'));
   end
             
	%{

	%Try all possible values of pcp and store the combination with highest throughput

	%throughputs = zeros(32*32*32);
	maximum = 0;
	 for conc = 1:32
	     for par = 1:32
		 for pipe = 0:32
		     estimatedThr = bestMatchEquation([conc,par,pipe]);
		     %throughputs(conc*par*pipe) =estimatedThr;
		     if estimatedThr > maximum
		         maximum = estimatedThr;
		         opt = [conc,par,pipe];
		     end
		 end
	     end
	 end
	 maximum
	 opt
	 %plot(throughputs)

	%}

end

 
