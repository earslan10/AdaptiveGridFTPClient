%clear; clc; format compact; close all

function [final,val] = main(folderID, targetThroughput, trialNumber, sampleValues, maxValues, output_dir )
    maxEvaluatedDegree = 5;
	%fid = fopen('results.txt', 'w');
    targetThroughput = targetThroughput/(1000*1000);
	bestTrial = 0;
    bestTrialDegree = 1;
	minEstimationError = realmax;
    estimations = zeros(trialNumber,1);
    
    %bestEquationOfTrials = zeros(trialNumber);
    options = optimset('Display','off');
    
    reasonableTrials = 0;
	folderName = strcat(output_dir,'/chunk_',num2str(folderID),'/trial-');
	for trial = 0:trialNumber
        disp(['Trial# ', num2str(trial)]);
	    %fprintf(fid, 'Trial# %d\n', (trial+1));
	    
	    filename = strcat(folderName, num2str(trial),'.txt');
	    R2 = 0;
        maximumRsquare = 0;
	    %evaluate various levels of equation degree 
        bestDegree = 1;
        %currentRMSE = realmax;
	    for degree= 1:maxEvaluatedDegree
            
            [equation, R2, RMSE] = findEquation(filename, degree);
           
            f = inline(equation);
            %objectiveFunction = inline([' -1 *(' char(equation) ')']);
            %[t,val] = fmincon(objectiveFunction,[1,1,0],[],[],[],[],[1,1,0], maxValues , [], options);
            estimation = f(sampleValues);
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
                 disp(strcat('Trial:',num2str(bestTrial) , ' estimation:', num2str(estimation),...
                     ' error:', num2str(minEstimationError)));
            end
        else
            disp(strcat('Skipping:',num2str(trial) , ' estimation:', num2str(estimation),...
                     ' error:', num2str(abs(targetThroughput - estimation))));
        end
    end
    
   %return
    
    estimations
  
	filename = strcat(folderName, num2str(bestTrial),'.txt');
	%[t,val] = fminsearch(inline(equation),[1, 1, 1]);
	%disp('various degrees')
    %{
	for degree= 1:3
	    [equation, R2] = findEquation(filename, degree);
	    R2
	    objectiveFunction = inline([' -1 *(' char(equation) ')']);
	    [t,val] = fmincon(objectiveFunction,[1,1,1],[],[],[],[],[0,0,0],[32,32,32])
	     %choose the one with highest R2
    end
    %}
    
    disp(strcat('Best trial:', num2str(bestTrial),' degree:', num2str(bestTrialDegree), ' error:', num2str(minEstimationError)));
    [equation, R2, RMSE] = findEquation(filename, bestTrialDegree);
    objectiveFunction = inline([' -1 *(' char(equation) ')']);
	[t,val] = fmincon(objectiveFunction,[1,1,0],[],[],[],[],[1,1,0], maxValues , [], options);
   
    t
	%bestMatchEquation([t(1),t(2),t(3)])
    
    
    stdev = std(estimations);
    avg = mean(estimations);
    disp(strcat('Min Error:', num2str(minEstimationError), 'Mean:', num2str(avg),' Stdev:', num2str(stdev)));
    %minEstimationError 
    
    disp(strcat('Trials error less than:', num2str(minEstimationError + stdev)));
    
    cc = 0;
    p = 0;
    ppq = 0;
    totalWeight = 0;
    for trial = 0:trialNumber
        
        %{
	    filename = strcat(folderName, num2str(trial),'.txt');
	    R2 = 0;
        maximumRsquare = 0;
	    %evaluate various levels of equation degree 
	    for degree= 1:maxEvaluatedDegree
            %disp(['Degree# ', num2str(degree)]);
            [equation, R2] = findEquation(filename, degree);
             %choose the one with highest R2
             if R2 > maximumRsquare
                 localBestMatchEquation  = equation;
                 maximumRsquare = R2;
             end
        end
        disp(strcat('Trial:',num2str(trial) , ' best-equation:', localBestMatchEquation));
        %}
        
        localBestMatchEquation = bestEquationOfTrials{trial+1};
        f = inline(localBestMatchEquation);
	    estimation = f(sampleValues);
        if abs(targetThroughput - estimation) <  10^4 
            %if minEstimationError + stdev   > abs(estimation- targetThroughput)*0.9
            if avg   > abs(estimation- targetThroughput)*0.9
                 objectiveFunction = inline([' -1 *(' char(localBestMatchEquation) ')']);
                 [t,val] = fmincon(objectiveFunction,[1,1,0],[],[],[],[],[1,1,0],maxValues,[], options);
                 %t
                 %val
                 if -1*val > 10^4
                      disp(strcat('Skipping:',num2str(trial) , ' estimation:', num2str(val)));
                      continue;
                 end
                 
                 weight = 1 / abs(estimation- targetThroughput);
                 disp(strcat('Final Trial#',num2str(trial) ,' estimation:'...
                     ,num2str(estimation),' error:', num2str(abs(estimation - targetThroughput)), ...
                     ' weight:',num2str(weight), 'Val:', num2str(val)));
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
                     newEstimation = f([subOptimalCC,t(2),t(3)]);
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
                 totalWeight = totalWeight + weight;
                 
                 disp(strcat('All Adjusted cc:',num2str(subOptimalCC), ' p:',...
                     num2str(subOptimalP), ' ppq:',num2str(subOptimalPPQ)));
                 
            end
        end
    end
    
    
    cc = round( cc / totalWeight);
    p = round( p / totalWeight);
    ppq = round( ppq/ totalWeight);

    final(1) = cc;
    final(2) = p;
    final(3) = ppq;
    val = f([cc,p,ppq])
    disp(strcat('Optimal cc:',num2str(cc) ,' p:', num2str(p), ' ppq:', num2str(ppq),...
                 ' total Weight:', num2str(totalWeight)));
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

 
