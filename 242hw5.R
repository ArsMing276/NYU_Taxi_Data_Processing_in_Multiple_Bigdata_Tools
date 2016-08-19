##Find deciles of all data

library('snow')
library('data.table')
file = list.files("D:/Rwd", pattern = "fare.*\\.csv$", full.names = TRUE)
cl = makeCluster(4)
clusterExport(cl, "fread")
els = clusterSplit(cl, file)

f = function(x){
  ##read in files without title
  total = as.data.frame(fread(sprintf('tail -n +2 -q %s|cut -f 11 -d, ', paste(x, collapse = ' '))))[,1]
  toll = as.data.frame(fread(sprintf('tail -n +2 -q %s|cut -f 10 -d ,', paste(x, collapse = ' '))))[,1]
  diff = as.numeric(total) - as.numeric(toll)
  freq = table(diff) 
  freq
}

cldiff = clusterApplyLB(cl, els, f)
save(cldiff, file = 'cldiff.rda')

nm = unique(names(do.call(c, cldiff))) 
nm = nm[order(as.numeric(nm))] ##find out what numbers are in the freq table
allfreq = rep(0, length(nm))
names(allfreq) = nm  ##make a big table with all numbers
for(i in 1:4){
  freq = rep(0, length(nm))
  freq[match(names(cldiff[[i]]), nm)] = cldiff[[i]]
  allfreq = allfreq + freq  ##extend small tables into a big one and combine all tables up.
}
allfreq

##finding out cumulative frequncy for numbers an deciles
##Then determine which group each decile should belong to.
cumfreq = cumsum(allfreq) 
deciles = seq(1, sum(allfreq), length.out = 11) 
groups = sapply(deciles, function(x) names(cumfreq[cumfreq <= x][sum(cumfreq <= x)]))
groups


##sampling to check if two data sets match
library('snow')
bigsample = function(filename){
  n = system(sprintf('wc -l %s', filename), intern = TRUE)
  n = as.numeric(strsplit(n, ' ')[[1]][1]) ##check row number for each file
  set.seed(242)
  ids = sample(2:n, 50000) ##skip title
  jumps = diff(c(0, sort(ids))) 
  con = file(filename, "r")  # connection
  isOpen(con)  # TRUE  - just checking
  
  ans = character(50000) ##sample 50000 obs for each file
  for(i in seq_along(jumps))
    ans[i] = readLines(con, jumps[i])[jumps[i]]
    j = match(ids, sort(ids))
    ans[j]
  
    ans
}

f = function(filename){
  samp = bigsample(filename)  ##get the 50000 samples
  if(grepl("fare", filename)) colind = c(1,2,4)
  else colind = c(1,2,6)      
  matsamp = sapply(strsplit(samp, ','), function(x) x[colind])##extract different columns accrording to file name and put into a matrix
  matsamp
}
  
tripfare = list.files("D:/Rwd", pattern = "fare.*\\.csv$", full.names = TRUE)
tripdata = list.files("D:/Rwd", pattern = "trip_data.*\\.csv$", full.names = TRUE)

cl1 = makeCluster(4)
clusterExport(cl1, "bigsample")
faresamp = parLapply(cl1, tripfare, f)
save(faresamp, file = 'faresamp.rda')

cl2 = makeCluster(4)
clusterExport(cl2, "bigsample")
datasamp = parLapply(cl2, tripdata, f)
save(datasamp, file = 'datasamp.rda')

##check if faresamp and datasamp are totally the same in the three columns
checkequal = sapply(1:12, function(x) all(faresamp[[x]] == datasamp[[x]]))

#############

##calculate b0 and b1 parallel version
library('snow')
library('stringr')
tripfare = list.files("D:/Rwd", pattern = "fare.*\\.csv$", full.names = TRUE)
tripdata = list.files("D:/Rwd", pattern = "trip_data.*\\.csv$", full.names = TRUE)
file = c(tripdata, tripfare)
pos = do.call(c, lapply(1:12, function(x) grep(sprintf('_%d.', x), file, fixed = TRUE)))
file = file[pos]  ##rearrange file order according to month, this is because we want each node have
                  ##trip_fare and trip_data of same months
cl = makeCluster(4)
clusterExport(cl, "str_trim")
els = clusterSplit(cl, file)

f = function(x){
  n = system(sprintf('wc -l %s', paste(c(x[2],x[4],x[6]), collapse = ' ')), intern = TRUE)
  n = as.numeric(strsplit(str_trim(n[4]), ' ')[[1]][1]) - 3 #-3 because of title!!!
  iter = n%/%5000000 + 1  ##check how many iteration read in blocks to the end of file
  
  xysum = 0
  xsum = 0
  ysum = 0
  xsqrsum = 0
  
  ##Three connection of 9,10,11 columns
  con1 = pipe(sprintf('tail -n +2 -q %s|cut -f 11 -d,', paste(c(x[2],x[4],x[6]), collapse = ' ')))
  con2 = pipe(sprintf('tail -n +2 -q %s|cut -f 10 -d,', paste(c(x[2],x[4],x[6]), collapse = ' ')))
  con3 = pipe(sprintf('tail -n +2 -q %s|cut -f 9 -d,', paste(c(x[1],x[3],x[5]), collapse = ' ')))
  for(i in 1:iter){
    fare11 = readLines(con1, 5000000)
    fare10 = readLines(con2, 5000000)
    data9 = readLines(con3, 5000000) ##get the data

    rsp = as.numeric(fare11) - as.numeric(fare10)
    rsp = rsp[!is.na(rsp)]
    pdt = as.numeric(data9)
    pdt = pdt[!is.na(pdt)] ##get predictor and response variables
  
    xysum = xysum + sum(pdt * rsp)
    xsum = xsum + sum(pdt)
    ysum = ysum + sum(rsp)
    xsqrsum = xsqrsum + sum(pdt^2) ## for each interation, adding these statistc up
  }
  
  stat = c(xysum, xsum, ysum, xsqrsum, n)
  names(stat) = c('xysum', 'xsum', 'ysum', 'xsqrsum', 'n')
  
  stat
}

clest = clusterApply(cl, els, f) ##This is a list of our statistics in each node
save(clest, file = 'clest.rda')
xystat = apply(do.call(rbind, clest), 2, sum) ##calculate the statistics in all nodes
beta1 = (xystat[1] - xystat[2] * xystat[3] / xystat[5])/(xystat[4] - xystat[2]^2 / xystat[5])
beta0 = xystat[3]/n - beta1 * xystat[2]/xystat[5]


####calculate b0 and b1 serialized version
library('stringr')
tripfare = list.files("D:/Rwd", pattern = "diff_.*\\.rda$", full.names = TRUE)
tripdata = list.files("D:/Rwd", pattern = "time_.*\\.rda$", full.names = TRUE)
file = c(tripdata, tripfare)
pos = do.call(c, lapply(1:12, function(x) grep(sprintf('_%d.', x), file, fixed = TRUE)))
file = file[pos]
xysum = 0
xsum = 0
ysum = 0
xsqrsum = 0

for(i in seq(1, 23, 2)){
  load(file[i])
  load(file[i+1])   
  
  xysum = xysum + sum(triptime * diff)
  xsum = xsum + sum(triptime)
  ysum = ysum + sum(diff)
  xsqrsum = xsqrsum + sum(triptime^2)
  
}

n = 173179759
beta1 = (xysum - xsum * ysum / n)/(xsqrsum - xsum^2 / n)
beta0 = ysum/n - beta1 * xsum/n

####beta1 = 2.06e-5, beta0 = 14.523####

####BLB to calculate se parallel version
library('snow')
library('stringr')
library('multinomRob')

##calculate row number for all files and the totle number
tripfare = list.files("D:/Rwd", pattern = "fare.*\\.csv$", full.names = TRUE)
pos = do.call(c, lapply(1:12, function(x) grep(sprintf('_%d.', x), tripfare, fixed = TRUE)))
tripfare = tripfare[pos]
wc = system(sprintf('wc -l %s', paste(tripfare, collapse = ' ')), intern = TRUE)
wc = sapply(strsplit(str_trim(wc), ' '), function(x) as.numeric(x[1]))
flen = wc[1:12] - 1 

n = wc[13] - 12  ##total number minus title lines
m = ceiling(n^0.6) ##subsample number
subsample = list()
for(i in 1:10) {
  set.seed(i)
  subsample[[i]] = sample(n, m)
}

##For resample, we can't sample directly, but we can use multinomial random generating function
##to get how many times each number in subsample observations be sampled
##we sample 1000 times because we have 10 subsample sets and in each set we have 100 resample data
resample = sapply(1:1000, function(x) {set.seed(x);rmultinomial(n, pr = rep(1/m, m))})
save(resample, file = 'resample.rda')

##check what rows(all files combined) are in the resampling result.
sampind = sort(unique(do.call(c, subsample))) 

##To determine which file each each row number belongs to and calculate the row number in 
##the corresponding file instead of the row number of all files combined
cumlen = cumsum(flen)
group = cut(sampind, c(0, cumlen), labels = FALSE)
grpsamp = list()
for(i in 1:12){
  grpsamp[[i]] = subset(sampind, group == i) - c(0, cumlen[1:11])[i]
}

f = function(filename){
  ##check which month the file is 
  which = which(sapply(1:12, function(x) grepl(sprintf('_%s.', x), filename, fixed = TRUE)))
  ids = grpsamp[[which]]
  jumps = diff(c(0, ids)) 
  len = length(ids)
  ans = character(len)
  
  
  ##get x or y based on which file is
  if(grepl("fare", filename)){
    con1 = pipe(sprintf('cut -f 11 -d, %s', filename))
    con2 = pipe(sprintf('cut -f 10 -d, %s', filename))
    readLines(con1, 1)
    readLines(con2, 1)
    for(i in seq_along(jumps)){
      line11 = readLines(con1, jumps[i])[jumps[i]]
      line10 = readLines(con2, jumps[i])[jumps[i]]
      ans[i] = as.numeric(line11) - as.numeric(line10)
    }
        
    
    ans
  }  
  else{
    con = pipe(sprintf('cut -f 9 -d, %s', filename))
    readLines(con, 1)
    for(i in seq_along(jumps))
      ans[i] = readLines(con, jumps[i])[jumps[i]]
    
    ans
  }
 
}

tripfare = list.files("D:/Rwd", pattern = "fare.*\\.csv$", full.names = TRUE)
tripdata = list.files("D:/Rwd", pattern = "trip_data.*\\.csv$", full.names = TRUE)
file = c(tripdata, tripfare)
pos = do.call(c, lapply(1:12, function(x) grep(sprintf('_%d.', x), file, fixed = TRUE)))
file = file[pos]
cl = makeCluster(6)
clusterExport(cl, "grpsamp")
sampdat = parLapply(cl, file, f)
save(sampdat, file = 'sampdat.rda')
######This is extremely slow thus not be considered.

library('stringr')
library('multinomRob')
library('plotrix')
tripfare = list.files("D:/Rwd", pattern = "fare.*\\.csv$", full.names = TRUE)
pos = do.call(c, lapply(1:12, function(x) grep(sprintf('_%d.', x), tripfare, fixed = TRUE)))
tripfare = tripfare[pos]
wc = system(sprintf('wc -l %s', paste(tripfare, collapse = ' ')), intern = TRUE)
wc = sapply(strsplit(str_trim(wc), ' '), function(x) as.numeric(x[1]))
flen = wc[1:12] - 1 

n = wc[13] - 12
m = ceiling(n^0.6)
subsample = list()
for(i in 1:10) {
  set.seed(i)
  subsample[[i]] = sample(n, m)
}


resample = sapply(1:1000, function(x) {set.seed(x);rmultinomial(n, pr = rep(1/m, m))})
save(resample, file = 'resample.rda')

sampind = sort(unique(do.call(c, subsample)))
cumlen = cumsum(flen)
group = cut(sampind, c(0, cumlen), labels = FALSE)
grpsamp = list()
for(i in 1:12){
  grpsamp[[i]] = subset(sampind, group == i) - c(0, cumlen[1:11])[i]
}


tripfare = list.files("D:/Rwd", pattern = "diff_.*\\.rda$", full.names = TRUE)
tripdata = list.files("D:/Rwd", pattern = "time_.*\\.rda$", full.names = TRUE)
file = c(tripdata, tripfare)
pos = do.call(c, lapply(1:12, function(x) grep(sprintf('_%d.', x), file, fixed = TRUE)))
file = file[pos]

xyvalue = list()
for(i in 1:12){
  ids = grpsamp[[i]]
  load(file[2 * i - 1])
  load(file[2 * i])
  x = triptime[ids]
  y = diff[ids]
  xyvalue[[i]] = data.frame(x = x, y = y)
}

##put the x and y value we got into a data frame as an table to match index, x and y
xyvalues = do.call(rbind, xyvalue)
indtbl = data.frame(sampind = sampind, x = xyvalues$x, y = xyvalues$y)

f = function(x){
  mch = match(x, indtbl$sampind)
  df = data.frame(xval = indtbl$x[mch], yval = indtbl$y[mch])
  
  df
}
subsampxy = lapply(subsample, f) ##get the x and y value from the former table for each subsample observation


findbeta = function(subsampxy, resample){
  beta1 = numeric(1000)
  beta0 = numeric(1000)
  for(i in 1:1000){
    ##for each resample dataset, the repetition times are counted as weight for the regression  
    ##We calculate beta0 and beta1 for each resample set according to their subsample group
    weight = resample[,i]
    group = cut(1:1000, breaks = 100 * (0:10), labels = FALSE)
    x = subsampxy[[group[i]]]$x
    y = subsampxy[[group[i]]]$y
    xysum = sum(x * y *weight)
    xsum = sum(x * weight)
    ysum = sum(y * weight)
    xsqrsum = sum(x^2 * weight)
    n = sum(weight)
    b1 = (xysum - xsum * ysum / n)/(xsqrsum - xsum^2 / n)
    b0 = ysum/n - b1 * xsum/n
    beta1[i] = b1
    beta0[i] = b0
  }
  betadf = data.frame(beta1 = beta1, beta0 = beta0)
  
  betadf
}

betadf = findbeta(subsampxy, resample)

##Finally, we calculate se for each subsample group and average them to get the final se for beta
beta1se = mean(tapply(betadf$beta1, group, std.error))
beta0se = mean(tapply(betadf$beta0, group, std.error))

######se for b1 and b0 are 6.2e-8, 0.0001)