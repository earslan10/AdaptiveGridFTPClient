classdef EntrySet
    %UNTITLED2 Summary of this class goes here
    %   Detailed explanation goes here
    
    properties(Access = public)
        bestFitEq = '';
        R2 = 0;
        maxParamValues = zeros(3,1);
        note = '';
        closeness = 0;
        optimalParams = zeros(3,1);
        maximumThroughput = 0;
    end
    
    
    
    methods
        function obj = EntrySet(bestFitEq, R2, maxParamValues, closeness, note)
            obj.bestFitEq = bestFitEq;
            obj.R2 = R2;
            obj.maxParamValues = maxParamValues;
            obj.note = note;
            obj.closeness = closeness;
        end
        function setOptimalParams(obj, params)
            obj.optimalParam = params;
        end
    end
    
end

