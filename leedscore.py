import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.activations import linear,relu,sigmoid
from sklearn.preprocessing import StandardScaler

DB_HOST='*****************************'
DB_NAME='************'
DB_USER='***'
DB_PASSWORD='*******************'

import psycopg2
from psycopg2 import sql

conn=psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
cur=conn.cursor()

query='''
        CREATE EXTENSION IF NOT EXISTS tablefunc;
    '''
cur.execute(query)
conn.commit()


query_crosstab = '''
 CREATE OR REPLACE VIEW MOST_RECENT_TIMESTAMPS AS
    SELECT "projectId",
       COALESCE("timestamp_QUESTIONNAIRE_SENT", 0) AS "timestamp_QUESTIONNAIRE_SENT",
        COALESCE("timestamp_PROPOSAL_SENT", 0) AS "timestamp_PROPOSAL_SENT",
       COALESCE("timestamp_QUESTIONNAIRE_RESENT", 0) AS "timestamp_QUESTIONNAIRE_RESENT",
       COALESCE("timestamp_QUESTIONNAIRE_ANSWERED_BY_LEAD", 0) AS "timestamp_QUESTIONNAIRE_ANSWERED_BY_LEAD",
       COALESCE("timestamp_ACCEPTED_PROPOSAL", 0) AS "timestamp_ACCEPTED_PROPOSAL",
       COALESCE("timestamp_DECLINED_PROPOSAL", 0) AS "timestamp_DECLINED_PROPOSAL"
    FROM crosstab(
      $$
      SELECT
        "projectId",
        "logEvent",
        MAX("createdAt") AS most_recent_time
      FROM
        "activities"
      WHERE
        "logEvent" IN ('QUESTIONNAIRE SENT','PROPOSAL_SENT', 'QUESTIONNAIRE RESENT', 'QUESTIONNAIRE_ANSWERED_BY_LEAD', 'ACCEPTED_PROPOSAL', 'DECLINED_PROPOSAL','QUESTIONNAIRE_LINK_VIEWED', 'QUESTIONNAIRE_POST_OPENED', 'QUESTIONNAIRE_OPENED', 'OPENED_PROPOSAL_ACTIVITY', 'PROPOSAL_LINK_VIEWED', 'OPENED_PROPOSAL', 'PROPOSAL_POST_OPENED', 'LEAD_ADDED_COMMENTS', 'DOWNLOADED_FILE_OR_FOLDER')
         AND "projectId" IS NOT NULL
      GROUP BY
        "projectId", "logEvent"
      ORDER BY
        "projectId", "logEvent"
      $$,
      $$
      VALUES ('QUESTIONNAIRE SENT'),('PROPOSAL_SENT'), ('QUESTIONNAIRE RESENT'), ('QUESTIONNAIRE_ANSWERED_BY_LEAD'), ('ACCEPTED_PROPOSAL'), ('DECLINED_PROPOSAL')
      $$
    ) AS ct("projectId" UUID, "timestamp_QUESTIONNAIRE_SENT" BIGINT,"timestamp_PROPOSAL_SENT" BIGINT, "timestamp_QUESTIONNAIRE_RESENT" BIGINT,
            "timestamp_QUESTIONNAIRE_ANSWERED_BY_LEAD" BIGINT, "timestamp_ACCEPTED_PROPOSAL" BIGINT, "timestamp_DECLINED_PROPOSAL" BIGINT);
'''
cur.execute(query_crosstab)



query='''
    CREATE OR REPLACE VIEW project_logevent_pivot AS
SELECT "projectId",
       COALESCE("QUESTIONNAIRE_SENT", 0) AS "QUESTIONNAIRE_SENT",
       COALESCE("PROPOSAL_SENT", 0) AS "PROPOSAL_SENT",
       COALESCE("QUESTIONNAIRE_RESENT", 0) AS "QUESTIONNAIRE_RESENT",
       COALESCE("QUESTIONNAIRE_LINK_VIEWED", 0) AS "QUESTIONNAIRE_LINK_VIEWED",
       COALESCE("QUESTIONNAIRE_POST_OPENED", 0) AS "QUESTIONNAIRE_POST_OPENED",
       COALESCE("QUESTIONNAIRE_OPENED", 0) AS "QUESTIONNAIRE_OPENED",
       COALESCE("QUESTIONNAIRE_ANSWERED_BY_LEAD", 0) AS "QUESTIONNAIRE_ANSWERED_BY_LEAD",
       COALESCE("OPENED_PROPOSAL_ACTIVITY", 0) AS "OPENED_PROPOSAL_ACTIVITY",
       COALESCE("PROPOSAL_LINK_VIEWED", 0) AS "PROPOSAL_LINK_VIEWED",
       COALESCE("OPENED_PROPOSAL", 0) AS "OPENED_PROPOSAL",
       COALESCE("PROPOSAL_POST_OPENED", 0) AS "PROPOSAL_POST_OPENED",
       COALESCE("LEAD_ADDED_COMMENTS", 0) AS "LEAD_ADDED_COMMENTS",
       COALESCE("DOWNLOADED_FILE_OR_FOLDER", 0) AS "DOWNLOADED_FILE_OR_FOLDER",
       COALESCE("DECLINED_PROPOSAL", 0) AS "DECLINED_PROPOSAL",
       COALESCE("ACCEPTED_PROPOSAL", 0) AS "ACCEPTED_PROPOSAL"
FROM crosstab(
  $$
  SELECT
    "projectId",
    "logEvent",
    COUNT(*)
  FROM
    "activities"
  WHERE
    "userId" IS NULL AND "createdBy" <> 'AUTOMATION' AND "projectId" IS NOT NULL
  GROUP BY
    "projectId", "logEvent"
  ORDER BY
    "projectId", "logEvent"
  $$,
  $$
  VALUES ('QUESTIONNAIRE_SENT'), ('PROPOSAL_SENT'), ('QUESTIONNAIRE_RESENT'),
         ('QUESTIONNAIRE_LINK_VIEWED'), ('QUESTIONNAIRE_POST_OPENED'),
         ('QUESTIONNAIRE_OPENED'),('QUESTIONNAIRE_ANSWERED_BY_LEAD'),
         ('OPENED_PROPOSAL_ACTIVITY'),('PROPOSAL_LINK_VIEWED'),('OPENED_PROPOSAL'),
         ('PROPOSAL_POST_OPENED'),('LEAD_ADDED_COMMENTS'),('DOWNLOADED_FILE_OR_FOLDER'),
         ('DECLINED_PROPOSAL'),('ACCEPTED_PROPOSAL')
  $$
) AS ct("projectId" UUID, "QUESTIONNAIRE_SENT" INT, "PROPOSAL_SENT" INT,
        "QUESTIONNAIRE_RESENT" INT, "QUESTIONNAIRE_LINK_VIEWED" INT,
        "QUESTIONNAIRE_POST_OPENED" INT,"QUESTIONNAIRE_OPENED" INT,
        "QUESTIONNAIRE_ANSWERED_BY_LEAD" INT,"OPENED_PROPOSAL_ACTIVITY" INT,
        "PROPOSAL_LINK_VIEWED" INT,"OPENED_PROPOSAL" INT,"PROPOSAL_POST_OPENED" INT,
        "LEAD_ADDED_COMMENTS" INT,"DOWNLOADED_FILE_OR_FOLDER" INT,"DECLINED_PROPOSAL" INT,
        "ACCEPTED_PROPOSAL" INT);
    '''
cur.execute(query)


query='''
        CREATE OR REPLACE VIEW combined_project_logevent AS
SELECT
    plp."projectId",
    plp."QUESTIONNAIRE_SENT",
    mrt."timestamp_QUESTIONNAIRE_SENT",
    mrt."timestamp_QUESTIONNAIRE_ANSWERED_BY_LEAD",
    plp."PROPOSAL_SENT",
    mrt."timestamp_PROPOSAL_SENT",
    plp."QUESTIONNAIRE_RESENT",
    mrt."timestamp_QUESTIONNAIRE_RESENT",
    plp."QUESTIONNAIRE_LINK_VIEWED",
    plp."QUESTIONNAIRE_POST_OPENED",
    plp."QUESTIONNAIRE_OPENED",
    plp."QUESTIONNAIRE_ANSWERED_BY_LEAD",
    plp."OPENED_PROPOSAL_ACTIVITY",
    plp."PROPOSAL_LINK_VIEWED",
    plp."OPENED_PROPOSAL",
    plp."PROPOSAL_POST_OPENED",
    plp."LEAD_ADDED_COMMENTS",
    plp."DOWNLOADED_FILE_OR_FOLDER",
    plp."DECLINED_PROPOSAL",
    plp."ACCEPTED_PROPOSAL",
    mrt."timestamp_DECLINED_PROPOSAL",
    mrt."timestamp_ACCEPTED_PROPOSAL"
FROM
    project_logevent_pivot plp
JOIN
    MOST_RECENT_TIMESTAMPS mrt ON plp."projectId" = mrt."projectId";


    '''
cur.execute(query)
query = '''
SELECT "projectId",
       "QUESTIONNAIRE_SENT" AS "questionnaireSent",
       ("timestamp_QUESTIONNAIRE_ANSWERED_BY_LEAD" - "timestamp_QUESTIONNAIRE_SENT") AS "timeTakenByLeadToRespond",
       "PROPOSAL_SENT" AS "proposalSent",
       "timestamp_PROPOSAL_SENT" as "timeStampOfProposalSent",
       "QUESTIONNAIRE_RESENT" AS "questionnaireResent",
       "timestamp_QUESTIONNAIRE_RESENT" AS "timeStampOfQuestionnaireResent",
       "QUESTIONNAIRE_LINK_VIEWED" AS "questionnaireLinkViewed",
       "QUESTIONNAIRE_POST_OPENED" AS "questionnairePostOpened",
       "QUESTIONNAIRE_OPENED" AS "questionnaireOpened",
       "QUESTIONNAIRE_ANSWERED_BY_LEAD" AS "questionnaireAnsweredByLead",
       "OPENED_PROPOSAL_ACTIVITY" AS "OpenedProposalActivity",
       "PROPOSAL_LINK_VIEWED" AS "proposalLinkViewed",
       "OPENED_PROPOSAL" AS "openedProposal",
       "PROPOSAL_POST_OPENED" AS "proposalPostOpened",
       "LEAD_ADDED_COMMENTS" AS "leadAddedComments",
       "DOWNLOADED_FILE_OR_FOLDER" AS "downloadedFileOrFolder",
       "DECLINED_PROPOSAL" AS "declinedProposal",
       "ACCEPTED_PROPOSAL" AS "acceptedProposal",
       "timestamp_DECLINED_PROPOSAL" AS "timeStampOfDeclinedProposal",
       "timestamp_ACCEPTED_PROPOSAL" AS "timeStampOfAcceptedProposal"
FROM combined_project_logevent;
'''

cur.execute(query)


df = pd.read_sql_query(query, conn)


df['timeTakenByLeadToRespond'] = np.where((df['timeTakenByLeadToRespond'] == 0) & (df['questionnaireSent'] != 0), df['timeTakenByLeadToRespond'].max(), df['timeTakenByLeadToRespond'])

X_train = df.iloc[:, 1:-4].values.astype('float32')
Y_train = np.where(df.iloc[:, -1] > df.iloc[:, -2], 1, 0).astype('float32')
Y_train = pd.Series(Y_train).values.astype('float32')
scaler = StandardScaler()

X_train_normalized = scaler.fit_transform(X_train)
# plt.figure(figsize = (15, 21))
# for i in np.arange(1, 16):
#     temp = X_train_normalized[:, i]
#     plt.subplot(7,3, i)
#     plt.boxplot(temp)
#     plt.title("Senor: "+ str(i))
# plt.show()

model=Sequential(
    [
        tf.keras.Input(shape=(16,)),
        Dense(16, activation ='relu', name="L1"),
        Dense(4, activation ='relu', name="L2"),
        Dense(1, activation ='sigmoid', name="L3")
    ]
)
model.summary()

[layer1,layer2,layer3] = model.layers
W1,b1 = layer1.get_weights()
W2,b2 = layer2.get_weights()
W3,b3 = layer3.get_weights()

model.compile(
    loss=tf.keras.losses.binary_crossentropy,
    optimizer=tf.keras.optimizers.Adam(learning_rate=0.002),
)

model.fit(
   X_train_normalized,Y_train,
    epochs=50,
    batch_size=8,
    shuffle=True,
    verbose=1
)

# layer_name = 'L2'
# intermediate_layer_model = Model(inputs=model.input,
# outputs=model.get_layer(layer_name).output)

# intermediate_output = intermediate_layer_model.predict(X_train_normalized)

# print(intermediate_output)




# fig, axs = plt.subplots(2, 2, figsize=(12, 10))

# # Feature 1 vs Y_train
# axs[0, 0].scatter(intermediate_output[:, 0], Y_train, color='r', label='Feature 1')
# axs[0, 0].set_xlabel('Feature 1')
# axs[0, 0].set_ylabel('Y_train')
# axs[0, 0].legend()
# axs[0, 0].set_title('Feature 1 vs Y_train')

# # Feature 2 vs Y_train
# axs[0, 1].scatter(intermediate_output[:, 1], Y_train, color='g', label='Feature 2')
# axs[0, 1].set_xlabel('Feature 2')
# axs[0, 1].set_ylabel('Y_train')
# axs[0, 1].legend()
# axs[0, 1].set_title('Feature 2 vs Y_train')

# # Feature 3 vs Y_train
# axs[1, 0].scatter(intermediate_output[:, 2], Y_train, color='b', label='Feature 3')
# axs[1, 0].set_xlabel('Feature 3')
# axs[1, 0].set_ylabel('Y_train')
# axs[1, 0].legend()
# axs[1, 0].set_title('Feature 3 vs Y_train')

# # Feature 4 vs Y_train
# axs[1, 1].scatter(intermediate_output[:, 3], Y_train, color='m', label='Feature 4')
# axs[1, 1].set_xlabel('Feature 4')
# axs[1, 1].set_ylabel('Y_train')
# axs[1, 1].legend()
# axs[1, 1].set_title('Feature 4 vs Y_train')

# plt.tight_layout()
# plt.show()


# for layer in model.layers:
#     weights = layer.get_weights()
#     if len(weights) > 0:
#         print(f"Layer: {layer.name}")
#         print(f"  Weights shape: {weights[0].shape}")
#         print(f"  Weights: {weights[0]}")
#         print(f"  Biases shape: {weights[1].shape}")
#         print(f"  Biases: {weights[1]}")

predictions = model.predict(X_test_normalized)

predictions

model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

loss, accuracy = model.evaluate(X_train, Y_train)
print("Loss:", loss)
print("Accuracy:", accuracy)

rounded_predictions = np.round(predictions*100)
rounded_predictions