fileId = fopen('/Users/earslan/HysterisisBasedMC/outputs//chunk_0/trial-31.txt');      
groupsNum = str2double(fgetl(fileId));
groups = zeros(groupsNum);
metadata = textscan(fileId, '%s %d\n',groupsNum );
data= textscan(fileId, '%f %f %f %f %f', 'headerLines', groupsNum+1, 'CommentStyle', '*');

matrix = cell2mat(data);
offset = 1;
for i = 1: groupsNum
    length = metadata{2}(i);
    test1 = matrix(offset:length,:);
    offset = offset + length;
end
