classdef EntrySet
    %UNTITLED2 Summary of this class goes here
    %   Detailed explanation goes here
    
    properties
        bestFitEq = '';
        R2 = 0;
        maxParamValues = zeros(3);
        note = '';
        closeness = 0;
    end
    
    methods
        function obj = EntrySet(bestFitEq, R2, maxParamValues, closeness, note)
            obj.bestFitEq = bestFitEq;
            obj.R2 = R2;
            obj.maxParamValues = maxParamValues;
            obj.note = note;
            obj.closeness = closeness;
        end
    end
    
end

