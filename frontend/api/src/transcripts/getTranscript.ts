import { HttpFunction } from '@google-cloud/functions-framework';
import { Request, Response } from 'express';
import axios from 'axios';


/**
 * HTTP Cloud Run Function which returns a transcript.
 * 
 * @param req 
 * @param res 
 */
export const getTranscript: HttpFunction = async (req: Request, res: Response) => {
  if (req.method === 'GET') {
    const apiUrl = process.env.TRANSCRIPT_API_URL;
    if (!apiUrl) {
      res.status(500).send('TRANSCRIPT_API_URL environment variable is not set');
      return;
    }

    try {
      const response = await axios.get(apiUrl);
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
