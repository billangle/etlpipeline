import {v4 as uuidv4} from 'uuid'; 
import {
	DynamoDBClient,
	PutItemCommand,
} from "@aws-sdk/client-dynamodb";
import {
	marshall,
	unmarshall
} from "@aws-sdk/util-dynamodb";

const ddb = new DynamoDBClient();

export const handler = async (event) => {
  // Accept either { jobDetails, timestamp } payload or the raw logged structure
  const jobDetails = event.jobDetails || (event.logged && event.logged.jobDetails) || event;
  const timestamp = event.timestamp || (event.logged && event.logged.timestamp) || new Date().toISOString();
  const jobId = jobDetails && (jobDetails.JobRunId || jobDetails.Id || jobDetails.jobRunId);
  const tableName = process.env.TABLE_NAME;

  console.log("PutGlueResult: event: " + JSON.stringify(event));


  if (!jobId) {
    throw new Error('Missing JobRunId in jobDetails');
  }
  const item = {
    jobId: jobId,
    jobState: jobDetails.JobRunState || jobDetails.jobRunState || null,
    timestamp: timestamp,
    fullResponse: jobDetails,
  };
  
    const putItem = new PutItemCommand({
                TableName: tableName,
                Item: marshall(item)
            });
            try {
                await ddb.send(putItem);
            } catch (e) {
                console.error ("ERROR: ImageProcess: saveToDynamo  : e: " + e);
            }
  
  return { ok: true, jobId };
};
