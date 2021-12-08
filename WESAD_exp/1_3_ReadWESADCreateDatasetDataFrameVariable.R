# WESAD preprocesing
library(reticulate)
library(parallel)
library(data.table)
dir="****"
dataset="WESAD/"
# Select by label, create the seq with que index to process and extrat the windows time series.
users=seq(2,17,1)[-11]

#Preprocessing parameters
step=0.25
accWin=5
phyWin=60

chestF=700
wFacc=32
wFbvp=64
wFeda=4
wFtemp=4

tsValues<-function(values,freq){
  ts(data = values,frequency = freq)
}

#Delete negative index
delNegatives<-function(x){
  out=x[x>0]
}

final=lapply(users,function(user){
  print(user)
  pd <- import("pandas")
  data <- pd$read_pickle(paste0(dir,dataset,"S",user,"/S",user,".pkl"))

  chest=as.data.frame(data$signal$chest)
  names(chest)=c("ACCx","ACCy","ACCz","ECG","EMG","EDA","TEMP","RESP")
  names(chest)=paste0("c_",names(chest))

  w_ACC=as.data.frame(data$signal$wrist$ACC)
  names(w_ACC)=c("w_ACCx","w_ACCy","w_ACCz")

  w_BVP=data$signal$wrist$BVP
  w_EDA=data$signal$wrist$EDA
  w_TEMP=data$signal$wrist$TEMP

  labels=c(1,2,3)
  output=lapply(labels,function(label){
    print(label)
    casos=which(data$label==label)
    start=head(casos,1)/700
    end=(tail(casos,1)/700)#Limited to the biggest window

    poss=seq(start,end,0.25)

    cases=c("c_ACCx","c_ACCy","c_ACCz" ,"c_ECG",
            "c_EMG","c_EDA","c_TEMP","c_RESP",
            "w_ACCx","w_ACCy","w_ACCz","w_BVP",
            "w_EDA","w_TEMP")
    out=mclapply(1:length(poss),function(posa){
      pos=poss[posa]

      if((sum(round((pos*chestF-phyWin*chestF+1):(pos*chestF))<0)>0)){
        print(paste0(user,"_negatives"))
      }
      ts <-tibble::tibble(
        subject=user,
        tsId=1,
        class=label,
        c_ACCx=list((chest[delNegatives(round((pos*chestF-accWin*chestF+1):(pos*chestF))),c("c_ACCx")])),
        c_ACCy=list((chest[delNegatives(round((pos*chestF-accWin*chestF+1):(pos*chestF))),c("c_ACCy")])),
        c_ACCz=list((chest[delNegatives(round((pos*chestF-accWin*chestF+1):(pos*chestF))),c("c_ACCz")])),
        c_ECG=list((chest[delNegatives(round((pos*chestF-phyWin*chestF+1):(pos*chestF))),c("c_ECG")])),
        c_EMG=list((chest[delNegatives(round((pos*chestF-phyWin*chestF+1):(pos*chestF))),c("c_EMG")])),
        c_EDA=list((chest[delNegatives(round((pos*chestF-phyWin*chestF+1):(pos*chestF))),c("c_EDA")])),
        c_TEMP=list((chest[delNegatives(round((pos*chestF-phyWin*chestF+1):(pos*chestF))),c("c_TEMP")])),
        c_RESP=list((chest[delNegatives(round((pos*chestF-phyWin*chestF+1):(pos*chestF))),c("c_RESP")])),
        w_ACCx=list((w_ACC[delNegatives(round((pos*wFacc-accWin*wFacc+1):(pos*wFacc))),c("w_ACCx")])),
        w_ACCy=list((w_ACC[delNegatives(round((pos*wFacc-accWin*wFacc+1):(pos*wFacc))),c("w_ACCy")])),
        w_ACCz=list((w_ACC[delNegatives(round((pos*wFacc-accWin*wFacc+1):(pos*wFacc))),c("w_ACCz")])),
        w_BVP=list((w_BVP[delNegatives(round((pos*wFbvp-phyWin*wFbvp+1):(pos*wFbvp)))])),
        w_EDA=list((w_EDA[delNegatives(round((pos*wFeda-phyWin*wFeda+1):(pos*wFeda)))])),
        w_TEMP=list((w_TEMP[delNegatives(round((pos*wFtemp-phyWin*wFtemp+1):(pos*wFtemp)))]))
      )
      ts
    },mc.cores = 10)
    out
  }
  )
  output=do.call(c,output)
  output=do.call(rbind.data.frame,output)
  output["tsId"]=1:dim(output)[1]

  saveRDS(output,file = paste0("/****/","WESAD_S",user,".RDS"))
  1
})

# CSV by user and variable.
myPaths <- .libPaths()
myPaths <- c("/****/R/x86_64-pc-linux-gnu-library/3.6/",myPaths)
.libPaths(myPaths)
.libPaths()
library(reticulate)
library(parallel)
library(data.table)
library(dplyr)
dir="/****/"
dataset="WESAD/"
users=seq(2,17,1)[-11]

outputDir="/****/"
final=lapply(users,function(user){
  print(user)
  data <- readRDS(paste0(outputDir,"WESAD_S",user,".RDS"))
  cases=c("c_ACCx","c_ACCy","c_ACCz" ,"c_ECG",
          "c_EMG","c_EDA","c_TEMP","c_RESP",
          "w_ACCx","w_ACCy","w_ACCz","w_BVP",
          "w_EDA","w_TEMP")
  cases=c("c_TEMP")
  mclapply(cases,function(col){
    print(col)
    data=data[,c(names(data)[1:3],col)]
    z=lapply(1:dim(data)[1],function(x){
      cbind(as.data.frame(data[x,1:3]),t(data.frame(unlist(data[x,col]))))
    })
    z=bind_rows(z)
    print(paste0("print_",col))
    fwrite(z, file = paste0(outputDir,"VarDataFrameNA/","WESAD_S",user,"_Var_",col,".csv"),sep = ",",append=F,na = "NA")
    },mc.cores = 8,mc.preschedule = T)
})

##After preparing the data in the desired format, we process the feature calculation in Spark in a distributed manner.
##scmftsSpark.scala

# Cleaning of the output dataset (Aux)
data=read.csv("/****/WESAD_CMFTS_normal_RAW_numeric.csv")

# Remove 71 columns with a single value
colQuitar=apply(data,2,function(x){
  length(unique(x))<=1
})
data=data[,!colQuitar]
data$class=as.factor(data$class)

# Imputation Na and NaN with mean per subject. Inf and -Inf with maximum and minimum per subject.
library(parallel)
library(gtools)
library(dplyr)
subjects=c(2:11,13:17)
results=lapply(subjects,function(subject){
  out=data[data[,1]==subject,]
  
  print(subject)
  out[,-c(1,2,3)]=apply(out[,-c(1,2,3)],2,function(z){

    x=z
    x[x==Inf]=max(x[is.finite(x)])
    x[x==-Inf]=min(x[is.finite(x)])
    x[!is.finite(x)]<-NA
    na.replace(x, mean(x,na.rm = TRUE))
  })
  out
})
x=bind_rows(results)
write.csv(x,"/****/WESAD_CMFTS_normal_RAW_numeric_inputedNA.csv",
          row.names = F)

## Model generation and final classification
##ScalaModelsWESAD.scala