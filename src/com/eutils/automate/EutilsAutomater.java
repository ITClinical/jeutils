package com.eutils.automate;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.eutils.Entrez;
import com.eutils.EFetch;
import com.eutils.ESearch;
import com.eutils.util.OutputListener;
import com.eutils.util.NotificationThread;
import com.eutils.util.ThreadListener;
import com.eutils.io.InputStreamParser;

import java.util.ArrayList;
/**
 * An automater to allow several queries to the NCBI servers to be performed at once. To automate several
 * queries to NCBI. create a new instance of this object with an array of searchable terms. One can then 
 * set variables specific to this search (database, format, etc...) using the functions inherted
 * from Entrez.</p>
 * <p>By default, downloaded EFetch data is sent to any OutputListeners registered with this object, or printed
 * to the command line if no listeners have been registered. One can also implement a custom parser
 * of the eFetch data, using the setParser function. In this case the data is parsed by the custom
 * parser and no data is sent to the listeners. </p>
 * The minimal implementation of this object is to a) create the object with search terms and b) calling start.
 * Under these conditions all efetch data is printed to the command line. 
 * Note: This object should not be re-used after start has been called.
 * @author Greg Cope
 */
public class EutilsAutomater extends Entrez implements InputStreamParser{

	/**
	 * A millisecond value to sleep between queries. NCBI recommends 3 seconds (3000) between queries.
	 */
	private static final int MIN_SLEEP_PATTERN = 3000;
	/**
	 * A list of output listeners who will be notified of data/errors/etc..
	 * Note: if a custom parser is set, these listeners will NOT be notified of incoming data.
	 */
	private ArrayList<OutputListener> outputListeners = new ArrayList<OutputListener>();
	
	/**
	 * The thread that actually runs the queries.
	 */
	private final EntrezThread entrezThread = new EntrezThread();
	
	/**
	 * An input stream parser object that can be used to parse any eFetch downloads. If
	 * this object is set to anything other than null (default), any OutputListeners will NOT be notified
	 * of data unless that custom implementation implements that behavior.
	 */
	private InputStreamParser parser = null;
	
	/**
	 * Allows multitple terms to be search at a single time. This value may be useful
	 * if one wishes to retrieve entries for values such as accession numbers.
	 */
	private int maxRetrieval = 1;
	
	/**
	 * The maximum number of errors that can occur before the progress of this object terminates.
	 */
	private int maxErrorCount = 4;
	
	/**
	 * Indicates if we've started our searches yet. 
	 */
	private boolean running = false;
	
	/**
	 * The terms to use for a search.
	 */
	String[] searchTerms;
	
	private EutilsAutomater(){
		super();
		setDefaultParameters();
	}
	/**
	 * Constructs a new Automater object that can be used to query NCBI for the terms specified.
	 * @param terms A list of terms to search NCBI.
	 */
	public EutilsAutomater(String[] terms){
		this();
		if ( terms.length < 1 ){
			throw new IllegalArgumentException("Cannot perform an automated query on < 1 terms.");
		}
		this.searchTerms = terms;
	}
	
	/**
	 * Initializes and starts this automation.
	 */
	public void start(){
		(new Thread(entrezThread)).start();
	}
	
	/**
	 * An inner class that actually runs the automater. This is a runnable interface
	 * that allows automation to be performed in a new thread and allows listeners to be notified
	 * upon start and termination.
	 * @author Greg Cope
	 *
	 */
	private class EntrezThread extends NotificationThread{
		
		public void doRun(){
			running = true;
			int errorCount = 0, searchCount = 0;

			for ( int i = 0; i < searchTerms.length; i += maxRetrieval ){
				ESearch search = new ESearch(parameters);
				search.setTerm(getTermsAsQueryString(i, maxRetrieval));
				try{
					search.doQuery(search);
				}catch (IOException io ){
					errorCount++;
					if ( validateErrorMax(errorCount)){
						return;
					}
					i -= maxRetrieval;
					sleep();
					continue;
				}
				if ( search.getIds() == null || search.getIds().length() == 0 ){
					searchCount+=maxRetrieval;
					sleep();
					continue;
				}
				EFetch fetch = new EFetch(search);
				try{
					if ( parser == null ){
						fetch.doQuery(EutilsAutomater.this);
					}else{
						fetch.doQuery(parser);
					}
				}catch ( IOException io ){
					errorCount++;
					if ( validateErrorMax(errorCount)){
						return;
					}
					i -= maxRetrieval;
					sleep();
					continue;
				}
				searchCount+=maxRetrieval;
				sleep();
			}	
			while ( true ){
				ESearch search = new ESearch(parameters);
				search.setTerm(getTermsAsQueryString(searchCount, searchTerms.length));
				try{
					search.doQuery(search);
				}catch (IOException io ){
					errorCount++;
					if ( validateErrorMax(errorCount)){
						return;
					}
					sleep();
					continue;
				}
				if ( search.getIds() == null || search.getIds().length() == 0 ){
					searchCount+=searchTerms.length - searchCount;
					searchCount = searchTerms.length;
					break;
				}
				EFetch fetch = new EFetch(search);
				try{
					if ( parser == null ){
						fetch.doQuery(EutilsAutomater.this);
					}else{
						fetch.doQuery(parser);
					}
				}catch ( IOException io ){
					errorCount++;
					if ( validateErrorMax(errorCount)){
						return;
					}
					sleep();
					continue;
				}
				searchCount += maxRetrieval;
				break;
			}
			running = false;
		}
	}
	
	/**
	 * Sleeps the calling thread for MIN_SLEEP_PATTERN milliseconds. 
	 */
	private void sleep(){
		try{
			Thread.sleep(MIN_SLEEP_PATTERN);
		}catch ( InterruptedException e ){
			
		}
	}
	/**
	 * Checks if the number or errors has exceeded the number tolerated, and if so fires all 
	 * the OutputListeners error method.
	 * @param max The number of errors that have occurred.
	 * @return true if max is greater than maxErrorCount, calse otherwise.
	 */
	private boolean validateErrorMax(int max){
		if ( max > maxErrorCount ){
			fireListenersError("Maximum error count exceeded - terminating queries.");
			return true;
		}
		return false;
	}
	/**
	 * Retrieves terms as a comma-seperated string. This is called from the EntrezThread
	 * to retrieve single or multiple values of searchTerms. All spaces are replaced with UTF-8 encoding
	 * (%20).
	 * @param start
	 * @param length
	 * @return A string that can be used in an esearch term value.
	 */
	private String getTermsAsQueryString(int start, int length){
		StringBuffer sb = new StringBuffer();
		for ( int i = start; i < length; i++ ){
			sb.append(searchTerms[i].trim().replaceAll("[\\s\\t]+", "%20"));
			sb.append(",");
		}
		sb.delete(sb.length()-1, sb.length());
		return sb.toString();
	}
	/**
	 * Notifies all output listeners that an error or type notice occurred. This error will not
	 * terminate the progress. 
	 * @param error
	 */
	private void fireListenersNotice(String error){
		for ( int i = 0; i < outputListeners.size(); i++ ){
			outputListeners.get(i).notice(error);
		}
	}
	/**
	 * Notifies all listeners that an error has occurred which has forced the termination
	 * of the progress of this automater. This is fired under situations where multiple attempts
	 * to retrieve an esearch or efetch failed. 
	 * @param error A String representation of the error that occurred. 
	 */
	private void fireListenersError(String error){
		for ( int i = 0; i < outputListeners.size(); i++ ){
			outputListeners.get(i).error(error);
		}
	}
	/**
	 * Sends data to particular listeners. This function sends data that has been downloaded by an eFetch.
	 * If a custom parser has been set, this function will not be called and listeners will not be notified
	 * of incoming data.
	 * @param data
	 */
	private void fireListenersData(String data){
		for ( int i = 0; i < outputListeners.size(); i++ ){
			outputListeners.get(i).data(data);
		}
	}	
	
	/**
	*Sets any default parameters. This is called from the main constructor, or any object that wishes
	*to re-use this object (in which case reset() should be called prior to this funcion).
	*Database: Pubmed
	*UseHistory: y
	*Retrieval Mode: XML
	*Does not print eSearch output to the terminal.
	*Does not prints eFetch output to the terminal.
	*/
	private void setDefaultParameters(){
		setDatabase(DB_PUBMED);
		setUseHistory("n");
		setRetType("xml");
		setPrintESearchOutput("n");
		setPrintEFetchOutput("n");
		setTool("J-eUtils");
		setEmail("gregcope@algosome.com");
	}	
	
	/**
	 * Adds an output listener to this object. This listener will be notified of events of certain events
	 * during the runtime of this object.
	 * @param listener An OutputListener to listen for output from this object.
	 */
	public void addOutputListener(OutputListener listener){
		this.outputListeners.add(listener);
	}

	/**
	 * Removes a particular listener from this object. 
	 * @param listener The OutputListener to remove.
	 */
	public void removeOutputListener(OutputListener listener){
		outputListeners.remove(listener);
	}
	
	/**
	 * Provides a means to know when this automate has started and finished.
	 * the entrez automater thread.
	 * @param listener The object to listener for start/termination events.
	 */
	public void addThreadListener(ThreadListener listener){
		entrezThread.addThreadListener(listener);
	}
	
	/**
	 * Adds a specific parser object. This object will be used to parse the eFetch output. A value of null
	 * will allow this automater object to search as the parser and print all output to the command line.
	 * NOTE: if parse is not null, any output listeners registered will NOT be notified of data being
	 * downloaded via efetch.
	 * @param parser
	 */
	public void setParser(InputStreamParser parser){
		this.parser = parser;
	}

	/**
	 * Removes a certain thread listener.
	 * @param listener
	 */
	public void removeThreadListener(ThreadListener listener){
		entrezThread.addThreadListener(listener);
	}	
	/**
	*Processes an eFetch Entrez Output. This function sends all efetch output to any 
	*registered listeners. If no listeners are registered, this prints to the command line.
	*@param is An InputStream to read from.
	*@throws IOException of the Input Stream could not be read, or the output was emtpy.
	*@see com.eutils.io.InputStreamParser
	*@see com.eutils.JeUtils for directions to set output flags.
	*/
	public void parseInput(InputStream is) throws IOException{
		BufferedReader br = null;
		try{
			StringBuffer sb = new StringBuffer();
			br = new BufferedReader( new InputStreamReader(is));
			String line = null;
			while ( (line = br.readLine() ) != null ){
				sb.append(line);
				sb.append("\n");
			}
			fireListenersData(sb.toString());
			if ( outputListeners.size() == 0 ){
				System.out.println(line);
			}
		}catch ( IOException io ){
			throw io;
		}finally{
			if ( br != null ){
				try{br.close();}catch ( IOException io ){}
			}
		}
	}
	/**
	*Empty implementation of the InputStreamParser
	*@param start The location to start from.
	*/
	public void parseFrom(int start){}
	/**
	*Empty implementation of the InputStreamParser.
	*@param end The location to stop at.
	*/
	public void parseTo(int end){}
	
	/**
	 * This object by default performs only a single query per search term. However, it can sometimes
	 * be more efficient to perform several queries at the same time (this may be the case for 
	 * retrieving records based upon single terms, such as accession numbers). Because some records
	 * may be large, the maximum value this can be set to is 100, otherwise errors such 
	 * as denial of service or download exceptions may occur more frequently.
	 * 
	 * @param max The maximum number of terms to send to NCBI at once.
	 * @throws IllegalArgumentException if max is less than 1 or greater than 100.
	 */
	public void setMaxRetrieval(int max){
		if ( max < 1 || max > 100){
			throw new IllegalArgumentException("Maximum retrieval must be greater than zero and less than 100.");
		}
		maxRetrieval = max;
	}
	
	/**
	 * Sets the maximum number of errors that are allowed before termination of progress. During
	 * progression of this automater, errors may occur. This object is designed to withstand certain
	 * errors however after so many errors the object will terminate. This function sets the maximum
	 * number of errors tolerated prior to termination. This value cannot be set after start has been
	 * called, or an IllegalStateException will the thrown.
	 * @param count The number of tolerated errors.
	 * @throws IllegalArgumentException if count is less than 1
	 * @throws IllegalStateException if the thread has already started.
	 */
	public void setMaxErrorCount(int count){
		if ( count < 1 ){
			throw new IllegalArgumentException("Maximum error count must be greater than zero.");
		}
		if ( running ){
			throw new IllegalStateException("Cannot set the max error count after thread has started");
		}
		maxErrorCount = count;
	}
	
	/**
	 * Checks if this object has started its queries. 
	 * @return true if it has started, false otherwise. 
	 */
	public boolean isRunning(){
		return running;
	}
	
}
