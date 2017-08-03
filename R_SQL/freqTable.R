# Wrapper for using xtable to write a summary table (counts and percentages) in 
# LaTeX format to file for incorporation into a project log
# Author: kimorley
###############################################################################

freqTable <- function(data, vars, byVar=NULL, varNames=NULL, returnTable=FALSE, latex=TRUE, excel=TRUE, fileName=NULL, chisqTest=FALSE, decimal=1){
    if ((latex==TRUE | excel==TRUE) & is.null(fileName)){
        stop('Please provide file name for latex/excel output file!')
    }
    if (!is.data.table(data)){
		data <- data.table(data)
	}
	if (!is.null(byVar)){	# N for total sample, or by subgroups 
		nTab <- table(data[, c(byVar), with=FALSE])	
	}else{
		nTab <- nrow(data)
	}
	if (is.null(varNames)){	# If user doesn't provide variable names, use those existing
		varNames <- vars
	}
    allTab <- data.frame() # Holds the results
	for (i in 1:length(vars)){
        # TOTAL SAMPLE
        tempTab1 <- table(data[, c(vars[i]), with=FALSE][[1]], useNA='ifany')
        tempTab2 <- prop.table(table(data[, c(vars[i]), with=FALSE][[1]], useNA='ifany'))*100
        if (sum(row.names(tempTab1) %in% c(NA))==1){
        	row.names(tempTab1)[length(row.names(tempTab1))] <- 'Missing'
        	row.names(tempTab2)[length(row.names(tempTab2))] <- 'Missing'
        }
        resTab <- data.frame(cbind(tempTab1, tempTab2))
        resTab <- cbind(row.names(resTab),resTab)     
        names(resTab) <- c('Categories', 'Total.N', 'Total.Perc')
		resTab <- data.frame(Variable=c(varNames[i],rep(NA,nrow(resTab)-1)), resTab)	
		if (!is.null(byVar)){	
            # SUBGROUP SUMMARIES
            rm(tempTab1,tempTab2)
            tempTab1 <- table(data[, c(vars[i], byVar), with=FALSE], useNA='ifany')
            tempTab2 <- prop.table(table(data[, c(vars[i],byVar), with=FALSE], useNA='ifany'), 2)*100
        	if (sum(row.names(tempTab1) %in% c(NA))==1){
        		row.names(tempTab1)[length(row.names(tempTab1))] <- 'Missing'
        		row.names(tempTab2)[length(row.names(tempTab2))] <- 'Missing'
        	}
            tempTab <- data.frame(cbind(tempTab1,tempTab2))
            # reorder so counts and percentages are paired for each subgroup
            colOrd <- c(); for (i in c(1:(ncol(tempTab)/2))){ colOrd <- c(colOrd, i, i+ncol(tempTab)/2) }
            tempTab <- data.frame(tempTab[,colOrd])
            for (i in seq(1, ncol(tempTab), 2)){
                names(tempTab)[i:(i+1)] <- c(paste(names(tempTab)[i],'N',sep='.'), paste(names(tempTab)[i],'Perc',sep='.'))
            }
            resTab <- cbind(resTab,tempTab)
		}
		allTab <- rbind(allTab, resTab)
	}
    if (latex){
        multiCol <- multiColNames(unlist(strsplit(grep('Perc', names(allTab), value=T),'.Perc')), nTab)
        colNames <- c('Characteristic','Categories',rep(c('N','\\%'),length(nTab)+1))
        temp <- allTab
        colnames(temp) <- colNames
        # Format table
        if (chisqTest){
            table <- xtable(temp, digits=c(0,0,0,rep(c(0,decimal),length(nTab)+1),3), align=c('l','l','l',rep('r',ncol(temp)-2)))
        }else{
            table <- xtable(temp, digits=c(0,0,0,rep(c(0,decimal),length(nTab)+1)), align=c('l','l','l',rep('r',ncol(temp)-2)))
        }
        print(table, 
              sanitize.text.function = function(x){x},
              floating=FALSE, 
              hline.after=NULL,
              size="\\footnotesize",
              include.rownames=FALSE,
              add.to.row=list(pos=list(-1,-1,0, nrow(table)),command=c('\\toprule ',multiCol,'\\midrule ','\\bottomrule ')),
              file=paste(fileName,'tex',sep='.')
        )
	}
	if (excel){
		write.csv(allTab, file=paste(fileName,'csv',sep='.'), row.names=FALSE)
	}
    if (returnTable){
		return(allTab)
    }
}

# EXTRA FUNCTIONS CALLED

# creates multicolumn names for latex
multiColNames <- function(grpLab, catTab){
    vec <- c()
    if (length(grpLab) > 1){
        for (k in 1:length(grpLab)){
            if (k == length(grpLab)){
                vec <- c(vec,paste(' \\multicolumn{2}{c}{',grpLab[k],' (',catTab[names(catTab)==grpLab[k]],')} \\\\ ',sep=''))
            }else if(k == 1){
                vec <- c(vec,paste('& & \\multicolumn{2}{c}{',grpLab[k],' (',sum(catTab),')} & ',sep=''))
            }else{
                vec <- c(vec,paste('\\multicolumn{2}{c}{',grpLab[k],' (',catTab[names(catTab)==grpLab[k]],')} & ',sep=''))
            }
        }    
    }else{
        if (chisqTest){
            vec <- c(vec,paste('& & \\multicolumn{2}{c}{',grpLab,' (',sum(catTab),')} & \\\\ ',sep=''))
        }else{
            vec <- c(vec,paste('& & \\multicolumn{2}{c}{',grpLab,' (',sum(catTab),')} \\\\ ',sep=''))
        }
    }
    vec <- paste(vec, collapse='')
    return(vec)
}
