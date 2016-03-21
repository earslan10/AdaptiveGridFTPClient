x = 1:10;
y = 11:20;
z = x'*y;
c = randi(10,10);

%surf(x,y,z,c)



file = load('/Users/earslan/data.txt');

cc = file(:,1);
p = file(:,2);
ppq = file(:,3);
thr = file(:,4);

scatter3(cc,p,ppq,thr);