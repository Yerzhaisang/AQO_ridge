#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>

#define WIDTH_0 15 // size of the input vector
#define WIDTH_1 100 // size of the output of the first layer
#define WIDTH_2 100 // size of the output of the second layer
#define lr 0.0001 // learning rate
#define slope 0.3 // parameter of non-activation layer

typedef struct NeuralNet {
	double **W1;
	double *b1;
	double **W2;
	double *b2;
	double *W3;
	double b3;
};

NeuralNet create_nn() {
	NeuralNet *res;

	MemoryContext	oldCxt;

	oldCxt = MemoryContextSwitchTo(AQOMemoryContext);
	res = palloc0(sizeof(NeuralNet));
	res->W1 = palloc(sizeof(res->W1[0]) * WIDTH_1);
	for (int i = 0; i < WIDTH_1; ++i) {
		res->W1[i] = palloc(sizeof(res->W1[0][0]) * WIDTH_0);
	}
	res->W2 = palloc(sizeof(res->W2[0]) * WIDTH_2);
	for (int i = 0; i < WIDTH_2; ++i) {
		res->W2[i] = palloc(sizeof(res->W2[0][0]) * WIDTH_1);
	}
	res->W3 = palloc(WIDTH_3 * sizeof(*W3));
	res->b1 = palloc(WIDTH_1 * sizeof(*b1));
	res->b2 = palloc(WIDTH_2 * sizeof(*b2));
	MemoryContextSwitchTo(oldCxt);

	return res;
}

void
nn_init (NeuralNet *nn){
    srand((long)time(NULL));
    double	stdv;
    stdv = 1 / sqrt(WIDTH_0);
    for (int i = 0; i < WIDTH_1; ++i)
        for (int j = 0; j < WIDTH_0; ++j){
            nn->W1[i][j] = (stdv + stdv)*(rand()/(double)RAND_MAX) - stdv;
    }
    srand((long)time(NULL));
    for (int i = 0; i < WIDTH_1; ++i)
        nn->b1[i] = (stdv + stdv)*(rand()/(double)RAND_MAX) - stdv;

    srand((long)time(NULL));
    stdv = 1 / sqrt(WIDTH_1);
    for (int i = 0; i < WIDTH_2; ++i)
        for (int j = 0; j < WIDTH_1; ++j){
            nn->W2[i][j] = (stdv + stdv)*(rand()/(double)RAND_MAX) - stdv;
    }
    srand((long)time(NULL));
    for (int i = 0; i < WIDTH_2; ++i)
        nn->b2[i] = (stdv + stdv)*(rand()/(double)RAND_MAX) - stdv;
    srand((long)time(NULL));
    stdv = 1 / sqrt(WIDTH_2);
    for (int i = 0; i < WIDTH_3; ++i)
        nn->W3[i] = (stdv + stdv)*(rand()/(double)RAND_MAX) - stdv;
    srand((long)time(NULL));
    nn->b3 = (stdv + stdv)*(rand()/(double)RAND_MAX) - stdv;
}

double
neural_predict (double **W1, double *b1, double **W2, double *b2, double *W3, double b3, double *feature){
    double *out1 = calloc(WIDTH_1, sizeof(*out1));
    for (int i = 0; i < WIDTH_1; ++i){
        for (int j = 0; j < WIDTH_0; ++j)
            out1[i] = out1[i]+feature[j]*W1[i][j];
        out1[i]=out1[i]+b1[i];
    }
    double *out2 = calloc(WIDTH_1, sizeof(*out2));
    for (int i = 0; i < WIDTH_1; ++i){
        if (out1[i]<out1[i]*slope)
            out2[i]=out1[i]*slope;
        else
            out2[i]=out1[i];
    }
    double *out3 = calloc(WIDTH_2, sizeof(*out3));
    for (int i = 0; i < 100; ++i){
        for (int j = 0; j < 100; ++j)
            out3[i]=out3[i]+out2[j]*W2[i][j];
        out3[i]=out3[i]+b2[i];
    }
    double *out4 = calloc(WIDTH_2, sizeof(*out4));
    for (int i = 0; i < WIDTH_2; ++i){
        if (out3[i]<out3[i]*slope)
            out4[i]=out3[i]*slope;
        else
            out4[i]=out3[i];
    }
    double out5=0;
    for (int j = 0; j < WIDTH_2; ++j)
        out5=out5+out4[j]*W3[j];
    out5=out5+b3;
    return out5;
}

void
neural_learn (double **W1, double *b1, double **W2, double *b2, double *W3, double b3, double *feature, double target, int l){
    for (int k = 0; k < l; ++k){
        double *out1 = calloc(WIDTH_1, sizeof(*out1));
        for (int i = 0; i < WIDTH_1; ++i){
            for (int j = 0; j < WIDTH_0; ++j)
                out1[i] = out1[i]+feature[j]*W1[i][j];
            out1[i]=out1[i]+b1[i];
        }
        double *out2 = calloc(WIDTH_1, sizeof(*out2));
        for (int i = 0; i < WIDTH_1; ++i){
            if (out1[i]<out1[i]*slope)
                out2[i]=out1[i]*slope;
            else
                out2[i]=out1[i];
        }
        double *out3 = calloc(WIDTH_2, sizeof(*out3));
        for (int i = 0; i < 100; ++i){
            for (int j = 0; j < 100; ++j)
                out3[i]=out3[i]+out2[j]*W2[i][j];
            out3[i]=out3[i]+b2[i];
        }
        double *out4 = calloc(WIDTH_2, sizeof(*out4));
        for (int i = 0; i < WIDTH_2; ++i){
            if (out3[i]<out3[i]*slope)
                out4[i]=out3[i]*slope;
            else
                out4[i]=out3[i];
        }
        double out5=0;
        for (int j = 0; j < WIDTH_2; ++j)
            out5=out5+out4[j]*W3[j];
        out5=out5+b3;
        double loss = pow(out5 - target,2);
        double dp1 = (out5 - target) * 2;
        double *gradW3=malloc(WIDTH_2 * sizeof(*gradW3));
        for (int i = 0; i < WIDTH_2; ++i)
            gradW3[i] = dp1 * out4[i];
        double *dp2=malloc(WIDTH_2 * sizeof(*dp2));
        for (int i = 0; i < WIDTH_2; ++i)
            dp2[i] = dp1 * W3[i];
        for (int i = 0; i < WIDTH_2; ++i)
            if (out3[i]<slope*out3[i])
                dp2[i] = dp2[i]*slope;
        double	*gradW2[WIDTH_2];
        for (int i = 0; i < WIDTH_2; ++i)
            gradW2[i] = malloc(sizeof(**gradW2) * WIDTH_1);
        for (int i = 0; i < WIDTH_2; ++i)
            for (int j = 0; j < WIDTH_1; ++j)
                gradW2[i][j]=dp2[i]*out2[j];
        double *dp3=calloc(sizeof(*dp3), WIDTH_1);
        for (int i = 0; i < WIDTH_1; ++i)
            for (int j = 0; j < WIDTH_2; ++j)
                dp3[j]+= dp2[j]*W2[j][i];
        for (int i = 0; i < WIDTH_1; ++i)
            if (out1[i]<slope*out1[i])
                dp3[i] = dp3[i]*slope;
        double	*gradW1[WIDTH_1];
        for (int i = 0; i < WIDTH_1; ++i)
            gradW1[i] = malloc(sizeof(**gradW1) * WIDTH_0);
        for (int i = 0; i < WIDTH_1; ++i)
            for (int j = 0; j < WIDTH_0; ++j)
                gradW1[i][j] = dp3[i] * feature[j];
        for (int i = 0; i < WIDTH_1; ++i){
            for (int j = 0; j < WIDTH_0; ++j)
                W1[i][j] = W1[i][j] - lr*gradW1[i][j];
            b1[i] = b1[i] - lr*dp3[i];
        }
        for (int i = 0; i < WIDTH_2; ++i){
            for (int j = 0; j < WIDTH_1; ++j)
                W2[i][j] = W2[i][j] - lr*gradW2[i][j];
            b2[i] = b2[i] - lr*dp2[i];
        }
        for (int i = 0; i < WIDTH_2; ++i)
            W3[i] = W3[i] - lr*gradW3[i];
        b3 = b3 - lr*dp1;
    }
}
