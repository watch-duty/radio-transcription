import { HttpFunction } from '@google-cloud/functions-framework';
import { Request, Response } from 'express';
import axios from 'axios';
import { GoogleAuth } from 'google-auth-library';



/**
 * HTTP Cloud Run Function which returns a list of transcripts for a feed ID.
 * 
 * @param req 
 * @param res 
 */
export const listTranscripts: HttpFunction = async (req: Request, res: Response) => {
  if (req.method === 'GET') {
    const apiUrl = process.env.TRANSCRIPT_API_URL;
    if (!apiUrl) {
      res.status(500).send('TRANSCRIPT_API_URL environment variable is not set');
      return;
    }

    // Get the Authentication token to allow us to call the Cloud Run function.
    const auth = new GoogleAuth();
    const client = await auth.getIdTokenClient(apiUrl);
    const tokenResponse = await client.getRequestHeaders();
    const token = tokenResponse.get('Authorization');

    const { feedId } = req.params;
    try {
      const response = await axios.get(apiUrl, { params: { feedId }, headers: { Authorization: token } });
      res.status(200).json(response.data);
    } catch (error: unknown) {
      if (error instanceof Error) {
        console.error('Error fetching transcript:', error);
        res.status(500).send(`Error fetching transcript: ${error.message}`);
      } else {
        console.error('Error fetching transcript:', error);
        res.status(500).send('Error fetching transcript');
      }
    }
  } else {
    res.status(405).send('Method Not Allowed');
  }
};
