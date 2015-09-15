function [equation,R2, RMSE, maxVals] = findEquation(data, degree)

     cc = data(:,1); 
     p = data(:,2); 
     ppq = data(:,3); 
     %fast = data(:,4);
     y = data(:,5); 
     maxVals = [max(cc), max(p), max(ppq)];
    
     if size(y) < 5
         R2 = 0;
         equation = '';
         RMSE = 10^10;
     else
        t= polyfitn([cc, p,ppq],y,degree);
        row= size(t.ModelTerms,1);
        equation = '';
        R2 = t.R2;
        RMSE = t.RMSE;
         % form the equation
         for i = 1:row
             power1 = t.ModelTerms(i,1);
             power2 = t.ModelTerms(i,2);
             power3 = t.ModelTerms(i,3);
             
             new = strcat('x(1).^',num2str(power1), '*', 'x(2).^',num2str(power2), '*', ...
                'x(3).^',num2str(power3) ,'*',num2str(t.Coefficients(i)));
            if i == 1
                equation = new;
            else
                equation = strcat (equation,'+',new);
            end
         end
     end
end