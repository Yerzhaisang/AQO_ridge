/*
 *******************************************************************************
 *
 *	MACHINE LEARNING TECHNIQUES
 *
 * This module does not know anything about DBMS, cardinalities and all other
 * stuff. It learns matrices, predicts values and is quite happy.
 * The proposed method is designed for working with limited number of objects.
 * It is guaranteed that number of rows in the matrix will not exceed aqo_K
 * setting after learning procedure. This property also allows to adapt to
 * workloads which properties are slowly changed.
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2020, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/machine_learning.c
 *
 */

#include "aqo.h"

double
rg_predict(int ncols, double *weights, double *features)
{
    int i;

    double res = 0;

    for (i=0;i<ncols;i++){
        res = res + features[i] * weights[i];
    }
    res = res + weights[ncols];
    return res;
}

int
rg_learn(int nfeatures, double *weights,
			double *features, double target)
{
	double *tgrad = malloc((nfeatures+1) * sizeof(double));
    	double htheta;
    	double err;
    	int j,k;

    	for (k=0;k<100;k++){
              htheta = 0;
              for (j=0;j<nfeatures;j++){
                 htheta = htheta + features[j]*weights[j];
              }
              htheta = htheta + weights[nfeatures];
	      err = htheta - target;
              for (j=0;j<nfeatures;j++){
                 tgrad[j] = 2*err*features[j]+2*0.001*weights[j];
                 weights[j] = weights[j] - 0.001*tgrad[j];
              }
              tgrad[nfeatures] = 2*err+2*0.001*weights[nfeatures];
              weights[nfeatures] = weights[nfeatures] - 0.001*tgrad[nfeatures];
           }


	return nfeatures;
}
