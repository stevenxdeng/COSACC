import numpy as np
from cvxopt import matrix, solvers
import json
import boto3
from decimal import *

dynamodb = boto3.resource('dynamodb')
output_table = dynamodb.Table('Speed_advisory')

#Temperary values
dt = 2.0

def Upload_Advisory(v_num,ID,s,adv):
    for i in range(v_num):
        if i > 0:
            ADV = round((adv[i]+s[i])/2,2)
            if ADV < 0:
                ADV = 0.0
            output_table.put_item(
                Item={
                    'vehicle': ID[i],
                    'advisory':Decimal(str(ADV))
                     }
            )
def lambda_handler(event, context):
    #Message from invocation
    v_num = event['VehicleNum']
    v_IDs = event['PlatoonID']
    v_h = list(map(float,event['PlatoonGap']))
    v_s = list(map(float,event['PlatoonSpeed']))

    #Temporary values
    Zeros = np.zeros([v_num,v_num])
    M_Identity = np.identity(v_num)

    #Matrix h and h reference
    h = np.array(v_h)
    h_ref = np.array(v_s)

    #Matrix Xa
    Xa = np.concatenate((h, h_ref), axis=0)

    #Constraints U_high and U_low
    S_max = 16.0
    a_acc = 2.0

    U_high = np.zeros(v_num)
    U_low = np.zeros(v_num)

    for vehicle in range(v_num):
        high_candidate_0 = 0.5 * (v_s[vehicle] + S_max)
        high_candidate_1 = v_s[vehicle] + 0.5 * a_acc * dt
        if high_candidate_0 <= high_candidate_1:
            U_high[vehicle] = high_candidate_0
        else:
            U_high[vehicle] = high_candidate_1

        low_candidate_0 = 0.5 * v_s[vehicle]
        low_candidate_1 = v_s[vehicle] + 0.5 * (-1) * a_acc * dt
        if low_candidate_0 >= low_candidate_1:
            U_low[vehicle] = low_candidate_0
        else:
            U_low[vehicle] = low_candidate_1

    #Matrix Phi
    Phi = np.identity(2*v_num)

    #Matrix Gamma
    B = Zeros
    for i in range(v_num):
        if i > 0:
            B[i][i] = (-1) * dt
            B[i][i-1] = dt
    Zeros = np.zeros([v_num,v_num])
    Gamma = np.vstack([B,Zeros])

    #Matrix Omega
    Ca = np.concatenate((np.identity(v_num), -1*np.identity(v_num)), axis=1)
    Omega = np.matmul(np.matmul(Ca.T,M_Identity),Ca)

    #Matrix M
    M = np.hstack([Zeros,Zeros]) #hstack: combine matrices horizontally
    M = np.vstack([M,M])         #vstack: combine matrices vertically
    M = np.vstack([M, np.hstack([-1*M_Identity, M_Identity])])
    M = np.vstack([M, np.hstack([M_Identity, -1*M_Identity])])

    #Matrix E0
    E0 = M_temp = np.vstack([-1*M_Identity, M_Identity])
    E0 = np.vstack([E0,Zeros])
    E0 = np.vstack([E0,Zeros])

    #Matrix D, Mu, and Sigma
    D = M
    Magic = np.zeros([4*v_num,2*v_num])
    Sigma = E0

    # Matrxi b0, b1, and C
    b0 = np.hstack([-1*U_low,U_high])
    b0 = np.hstack([b0,np.zeros(v_num)])
    b0 = np.hstack([b0, 10000.0*np.ones(v_num)]).reshape(4*v_num,1)
    C = b0

    #Matrix S and W
    S = np.matmul(Magic,Gamma) + Sigma
    W = -1 * (np.matmul(Magic, Phi) + D)

    #Matrix G and F
    G = np.matmul(np.matmul(Gamma.T,Omega),Gamma) * 2
    F = np.matmul(np.matmul(Gamma.T,Omega),Phi) * 2

    #Parameter for quadprog
    Param_1 = np.matmul(F,Xa)
    Param_1 = Param_1[:,np.newaxis]
    WXa = np.matmul(W,Xa)
    WXa = WXa[:,np.newaxis]
    Param_2 = WXa + C

    U = solvers.qp(matrix(G), matrix(Param_1), matrix(S), matrix(Param_2))

    Upload_Advisory(v_num,v_IDs,v_s,U['x'])
    return {"Success":"True"}
