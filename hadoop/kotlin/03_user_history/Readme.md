### User history
Input: csv file (exported from BQ, for example) with following header:
```
livestreamId,hostId,memberId,interactionDate,totalTimespentMins,livestreamExitTime,label,evaluationFlag,positivesHistory,negativesHistory
```
Performs reduce by "memberId". For each member, we "aggregate" the history  
of livestreams he/she has interacted with based on "label" -> positivesHistory, negativesHistory.  
History is just a list of up to 32 recent hostId's. All interactions are included with 1 hour delay to simulate runtime.  

Output: csv file with extra 2 columns - positive and negative history hostIds, space separated.  
Build and run:
```
./gradlew jar
hadoop jar build/libs/user_history-1.0-SNAPSHOT.jar --input "gs://gcs_bucket/path/*.csv.gz" -o  gs://gcs_bucket/output_dir
```
Can be run locally or on Dataproc cluster.