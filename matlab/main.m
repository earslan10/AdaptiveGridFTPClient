%clear; clc; format compact; close all
function [final,val] = main(filename, targetThroughput, sampleValues, testPcp, testThroughput )
    
    targetThroughput = targetThroughput/(1000*1000);
    options = optimset('Display','off');
	%folderName = strcat(output_dir,filename);
	%for trial = 1:1
    
    %bestEquationOfTrials =cell(size(trialNumber,2),1);
    %maxParamValues = cell(size(trialNumber,2),1);
    %R2Values = zeros(size(trialNumber,2),1);
    
    %Read input file
    fileId = fopen(filename);      
    numGroups = str2double(fgetl(fileId));
    metadata = textscan(fileId, '%s %d\n',numGroups );
    data= textscan(fileId, '%f %f %f %f %f', 'CommentStyle', '*');
    matrix = cell2mat(data);
    warning off;
    
    % Find equation for each entry set (es)
    offset = 1;
    %entrySetList(1,numGroups) = EntrySet();
    index = 1;
    for i = 1:numGroups
        name = cell2mat(metadata{1}(i));
        count = metadata{2}(i);
        %disp(strcat('name:', name, ' count:', num2str(count)));
        subMatrix = matrix(offset:offset + count -1,:);
        [equation, R2, ~, maxVals] = findEquation(subMatrix, 3);
        f = @(x)eval(equation);
        estimation = f([sampleValues(1), sampleValues(2), sampleValues(3)]); 
        closeness = abs(targetThroughput - estimation);
        if closeness <  10^3 && estimation <  10^3 && estimation >100  && R2 > 0.6
            disp(strcat('Adding:',name , ' estimation:', num2str(estimation),...
                ' error:', num2str(closeness), ' R2:', num2str(R2)));
            entrySetList(index) = EntrySet(equation, R2, maxVals, closeness, name);
            errors(index) = closeness;
            index = index + 1;
        else
            disp(strcat('Skipping:',name , ' estimation:', num2str(estimation),...
                ' error:', num2str(abs(targetThroughput - estimation)), ' R2:', num2str(R2)));
        end
        offset = offset + count;
    end
    if ~exist('entrySetList', 'var') | size(entrySetList) == 0
        %disp('No entry found similar to the target! Exiting...')
        return
    end
        
    %fprintf('Total entry set list l %d\n', size(entrySetList));
    %disp(strcat('TOTAL entry set list l', ' ', num2str(size(entrySetList))));
    
    
    %errors
    [idx,cntr] = kmeanspp(errors,5);
    %cntr
    %idx
    
    new_idx = zeros(5,1);
    [sorted_cntr] = sort(cntr, 'descend');
    for i = 1:5
        for j = 1:5
            if cntr(i) == sorted_cntr(j)
                new_idx(i) = j;
                break;
            end
        end
    end
    totalWeight = 0;
    index = 1;
    for entrySet = entrySetList
        newEq = strcat(' -1 *(', entrySet.bestFitEq ,')');
        objectiveF = @(x)eval(newEq);
        [t,val] = fmincon(objectiveF,[1,1,0],[],[],[],[],[1,1,0],entrySet.maxParamValues,[], options);
         if -1*val > 10^3 | -1*val < 10
            disp(strcat('SKIPPING:',num2str(entrySet.note) , ' estimation:', num2str(val)));
            continue;
        end
        weight = 2^(new_idx(idx(index))-1);
        %weight = targetThroughput / (targetThroughput + entrySet.closeness);
        %weight = 1 / entrySet.closeness;
        totalWeight = totalWeight + weight;
        index = index + 1;
    end
    %return
    
    %sumd = zeros(size(errors));
    %for i = 1:size(errors')
    %    sumd(i) = abs(errors(i) - cntr(idx(i)));
    %end
           
    %cntr
    %sorted_cntr
    %original_index
    %sumd
    cc = 0;
    p = 0;
    ppq = 0;
    totalErrorWeight = 0;
    totalThrouhput = 0;
    errorRate = 0;
    index = 0;
    for entrySet = entrySetList
        index = index + 1;
        %entrySet.maxParamValues
        %entrySet.bestFitEq
        newEq = strcat(' -1 *(', entrySet.bestFitEq ,')');
        objectiveF = @(x)eval(newEq);
        [t,val] = fmincon(objectiveF,[1,1,0],[],[],[],[],[1,1,0],entrySet.maxParamValues,[], options);
        if -1*val > 10^3 | -1*val < 10
            disp(strcat('SKIPPING:',num2str(entrySet.note) , ' estimation:', num2str(val)));
            continue;
        end
        %weight = targetThroughput / (targetThroughput + entrySet.closeness);
        %weight = (1 / entrySet.closeness);
        %entrySet.closeness
        weight_percent = 100 * weight / totalWeight;
        %new_idx(idx(index))
        weight = 2^(new_idx(idx(index)) -1 );
        %weight
        %disp(strcat('Final #',entrySet.note ,' error:', num2str(entrySet.closeness), ...
        %     ' R2:', num2str(entrySet.R2),' weight:',num2str(weight),...
        %    ' Val:', num2str(val)));
        disp(strcat('Fmincon cc:',num2str(t(1)) ,' p:', num2str(round(t(2))), ...
             ' ppq:', num2str(round(t(3))) , ' value:',num2str(-1*val)));
         
        f = @(x)eval(entrySet.bestFitEq);
        thrEstimation = -1 * val;
        
        for subOptimalCC = round(t(1)) : -1 :1
            newEstimation = f([subOptimalCC,t(2),t(3)]);
            %disp(strcat('CC:',num2str(subOptimalCC) ,' estimation:',num2str(newEstimation)));
            if newEstimation < thrEstimation * 0.8
                subOptimalCC = subOptimalCC + 1;
                %disp(strcat('Adjusted CC:',num2str(subOptimalCC) ,' estimation:',num2str(newEstimation)));
                break;
            end
         end
         
        thrEstimation = newEstimation;
        subOptimalP = round(t(2));
        if subOptimalP >= 1 
             for subOptimalP = round(t(2)) : -1 :0 
                 newEstimation = f([subOptimalCC,subOptimalP,t(3)]);
                 %disp(strcat('CC:',num2str(subOptimalCC) ,' estimation:',num2str(newEstimation)));
                 if newEstimation < thrEstimation  * 0.9
                     subOptimalP = subOptimalP + 1;
                     %disp(strcat('Adjusted P:',num2str(subOptimalP) ,' estimation:',num2str(newEstimation)));
                     break;
                 end
             end
        end
        
        thrEstimation = newEstimation;
        subOptimalPPQ = round(t(3));
        if subOptimalPPQ >= 1
            for subOptimalPPQ = round(t(3)) : -1 :0 
                newEstimation = f([subOptimalCC,subOptimalP,subOptimalPPQ]);
                %disp(strcat('Adjusted PPQ:',num2str(subOptimalPPQ) ,' estimation:',num2str(newEstimation)));
                if newEstimation > thrEstimation * 0.99
                    subOptimalPPQ = subOptimalPPQ -1;
                    %disp(strcat('Adjusted PPQ:',num2str(subOptimalPPQ) ,' estimation:',num2str(newEstimation)));
                    break;
                end
            end
        end
         cc = cc + subOptimalCC * weight;
         %p = p + t(2) * weight;
         p = p + subOptimalP * weight;
         %ppq = ppq + t(3) * weight;
         ppq = ppq + subOptimalPPQ * weight;

         totalThrouhput = totalThrouhput + f([subOptimalCC,subOptimalP,subOptimalPPQ]) * weight;

         %totalWeight = totalWeight + weight;

         disp(strcat('All Adjusted cc:',num2str(subOptimalCC), ' p:',...
             num2str(subOptimalP), ' ppq:',num2str(subOptimalPPQ), 'estimation:', num2str(newEstimation)));
           if isempty(testPcp) == 0
               maxParamValue = entrySet.maxParamValues;
               %testPcp
               if maxParamValue(1) < testPcp(1) || maxParamValue(2) < testPcp(2) || maxParamValue(3) < testPcp(3)
                   %disp(strcat('Skipping trial ', num2str(entrySet.note)));
               else
                   projectedMaxThroughput = f(testPcp);
                   localMaxThroughput = f([subOptimalCC,subOptimalP,subOptimalPPQ]);
                   localErrorRate = abs(localMaxThroughput - projectedMaxThroughput)/localMaxThroughput;
                   %testPcp
                   %disp(strcat('Projected :',num2str(projectedMaxThroughput), ' local',...
                   % num2str(localMaxThroughput), ' error:',num2str(localErrorRate)));
                   %testThroughput = testThroughput +  weight *  f(testPcp);
                   errorRate = errorRate + weight * localErrorRate;
                   totalErrorWeight = totalErrorWeight + weight;
               end
           end
    end
    cc
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
        errorRate = (errorRate/totalErrorWeight)  * 100;
        accuracy = 100 - errorRate;
        disp(strcat('Test throughput:',num2str(testThroughput) ,' accuracy:', num2str(accuracy), '%'));
        val = accuracy;
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


