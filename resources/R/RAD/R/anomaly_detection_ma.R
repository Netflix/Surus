AnomalyDetection.ma = function(X, frequency=7) {
  if (is.vector(X) & !is.data.frame(X)) X = data.frame(y=X)
  
  ma.ts = do.call('rbind', apply(X, 2, function(j) {
    j.matrix = matrix(j, nrow= frequency)
    means = apply(j.matrix[,1:(ncol(j.matrix)-1)], 1, mean)
    sds = apply(j.matrix[,1:(ncol(j.matrix)-1)],1,sd)
    upperbounds = means + 1.6*sds
    lowerbounds = means - 1.6*sds
    anomalous = t(apply(cbind(upperbounds, lowerbounds, j.matrix), 1, function(i) {
      i[-(1:2)] > i[1] | i[-(1:2)] < i[2]
    }))
    data.frame(X = j,
               time = 1:length(j),
               anomaly = as.vector(anomalous))
  }))
  ma.ts = cbind(ma.ts, name = rep(names(X), each = nrow(X)))
  
  return (ma.ts)
}

ggplot_AnomalyDetection.ma = function(anomalyDetection) {
  ggplot(anomalyDetection,
         aes(x = time, y=X)) +
    geom_line(size = 1) +
    geom_point(data = subset(anomalyDetection, anomaly == T), color = 'red', size = 6) +
    facet_wrap(~name, scale = 'free')
}