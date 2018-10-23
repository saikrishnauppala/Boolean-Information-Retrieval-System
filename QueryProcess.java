package queryProcessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;

public class QueryProcess
{
    public static Map<String, LinkedList<Integer>> dictionary = new HashMap<String, LinkedList<Integer>>();
    public static BufferedReader br;
    public static BufferedWriter bw;
    public static FileSystem fs = FileSystems.getDefault();
    public static Path pathobj;

    public static void getPostings(String[] terms) throws IOException
    {
	for (String term : terms)
	{
	    bw.write("GetPostings\r\n");
	    bw.write(term + "\r\n");
	    bw.write("Postings list: ");
	    for (int i : dictionary.get(term))
	    {
		bw.write(i + " ");
	    }
	    bw.write("\r\n");
	}
    }

    public static void taatOR(String[] terms) throws IOException
    {
	int tOrcomparisons = 0;
	LinkedList<Integer> result = dictionary.get(terms[0]);
	for (int t = 1; t < terms.length; t++)
	{
	    LinkedList<Integer> termpost = dictionary.get(terms[t]);
	    LinkedList<Integer> merge = new LinkedList<Integer>();
	    int i = 0, j = 0;
	    if (termpost.equals(null))
	    {
		continue;
	    }
	    if (result.equals(null))
	    {
		result = termpost;
	    }
	    while (i < result.size() && j < termpost.size())
	    {
		if (result.get(i) < termpost.get(j))
		{
		    tOrcomparisons++;
		    merge.add(result.get(i));
		    i++;
		}
		else if (result.get(i).equals(termpost.get(j)))
		{
		    tOrcomparisons++;
		    merge.add(result.get(i));
		    i++;
		    j++;
		}
		else
		{
		    tOrcomparisons++;
		    merge.add(termpost.get(j));
		    j++;
		}
	    }
	    while (i < result.size())
	    {
		merge.add(result.get(i));
		i++;
	    }
	    while (j < termpost.size())
	    {
		merge.add(termpost.get(j));
		j++;
	    }
	    result = merge;
	}
	bw.write("TaatOr\r\n");
	String tm = "";
	for (String s : terms)
	{
	    tm = tm + s + " ";
	}
	bw.write(tm + "\r\n");
	bw.write("Results: ");
	if (result.size() == 0)
	{
	    bw.write("empty");
	}
	else
	{
	    for (Integer i : result)
	    {
		bw.write(i + " ");
	    }
	}
	bw.write("\r\nNumber of documents in results: " + result.size() + "\r\n");
	bw.write("Number of comparisons: " + tOrcomparisons + "\r\n");
    }

    public static void taatAND(String[] terms) throws IOException
    {
	int tAndcomparisons = 0;
	LinkedList<Integer> result = dictionary.get(terms[0]);
	for (int t = 1; t < terms.length; t++)
	{
	    LinkedList<Integer> termpost = dictionary.get(terms[t]);
	    LinkedList<Integer> merge = new LinkedList<Integer>();
	    int i = 0, j = 0;
	    if (termpost.size() == 0 || result.size() == 0)
	    {
		break;
	    }
	    while (i < result.size() && j < termpost.size())
	    {
		if (result.get(i) < termpost.get(j))
		{	
		    int skiplength=(int)Math.sqrt(result.size());
		   // tAndcomparisons++;
		    if((i+skiplength)<(result.size()) && result.get(i+skiplength)<=termpost.get(j)) 
		    {//skip pointers
			
			   tAndcomparisons++;
			   i=i+skiplength;
			   System.out.println("skipped");
			  while((i+skiplength)<(result.size()) && result.get(i+skiplength)<=termpost.get(j))
			  {	tAndcomparisons++;
			      i=i+skiplength;
			     
			  }
		    }
		    else {
		    tAndcomparisons++;
		    i++;
		    }
		}
		else if (result.get(i).equals(termpost.get(j)))
		{
		    tAndcomparisons++;
		    merge.add(result.get(i));
		    i++;
		    j++;
		}
		else
		{
		    int skiplength=(int)Math.sqrt(termpost.size());
		    if((j+skiplength)<(termpost.size()) && termpost.get(j+skiplength)<=result.get(i)) 
		    {			
			   tAndcomparisons++;
			   j=j+skiplength;
			  while((j+skiplength)<(termpost.size()) && termpost.get(j+skiplength)<=result.get(i))
			  {	tAndcomparisons++;
			      j=j+skiplength;
			  }
		    }//skip pointers
		    else {
		    tAndcomparisons++;
		    j++;
		    }
		    
		}
	    }
	    result = merge;
	}
	bw.write("TaatAnd\r\n");
	String tm = "";
	for (String s : terms)
	{
	    tm = tm + s + " ";
	}
	bw.write(tm + "\r\n");
	bw.write("Results: ");
	if (result.size() == 0)
	{
	    bw.write("empty");
	    bw.write("\r\nNumber of documents in results: " + 0 + "\r\n");
	}
	else
	{
	    for (Integer i : result)
	    {
		bw.write(i + " ");
	    }
	    bw.write("\r\nNumber of documents in results: " + result.size() + "\r\n");
	}
	bw.write("Number of comparisons: " + tAndcomparisons + "\r\n");
    }

    public static void daatOR(String[] terms) throws IOException
    {
	int dOrcomparisons = 0;
	ArrayList<LinkedList<Integer>> termsOfQuery = new ArrayList<LinkedList<Integer>>();
	LinkedList<Integer> postings = new LinkedList<Integer>();
	LinkedList<Integer> reference = new LinkedList<Integer>();
	LinkedList<Integer> merge = new LinkedList<Integer>();
	int z = 0;
	int[] postingSize = new int[terms.length];
	for (int i = 0; i < terms.length; i++)
	{
	    postings = dictionary.get(terms[i]);
	    termsOfQuery.add(postings);
	    postingSize[i] = termsOfQuery.get(i).size();
	}
	int maxSize = postingSize[0];
	int maxPosting = 0;
	for (int i = 1; i < terms.length; i++)
	{
	    if (postingSize[i] > maxSize)
	    {
		maxSize = postingSize[i];
		maxPosting = i;
	    }
	}
	reference = termsOfQuery.get(maxPosting);
	termsOfQuery.add(0, reference);
	termsOfQuery.remove(maxPosting + 1);
	int size = termsOfQuery.size();
	int[] Pointers = new int[size];
	int[] Sizes = new int[size];
	for (int i = 0; i < termsOfQuery.size(); i++)
	{
	    Sizes[i] = termsOfQuery.get(i).size();
	    Pointers[i] = 0;
	}
	boolean end = false;
	while (end == false && termsOfQuery.size() != 1)
	{
	    int counter = 0;
	    for (int i = 0; i < size; i++)
	    {
		if (Pointers[i] == Sizes[i])
		    counter++;
	    }
	    if (counter == size)
		end = true;
	    if (end == false)
	    {
		int j = 0;
		while (j < size)
		{
		    if (Pointers[j] < Sizes[j])
		    {
			long a = termsOfQuery.get(j).get(Pointers[j]);
			boolean flag = true;
			dOrcomparisons++;
			for (int i = 0; i < merge.size(); i++)
			{
			    if (a == merge.get(i))
			    {
				flag = false;
				Pointers[j]++;
			    }
			}
			if (flag == true)
			{
			    merge.add(termsOfQuery.get(j).get(Pointers[j]));
			    Pointers[j]++;
			}
		    }
		    j++;
		}
	    }
	}
	while (termsOfQuery.size() == 1 && z < termsOfQuery.get(0).size())
	{
	    merge.add(termsOfQuery.get(0).get(z));
	    z = z + 1;
	}
	Collections.sort(merge);
	bw.write("DaatOr\r\n");
	bw.write("DaatAnd\r\n");
	String tm = "";
	for (String s : terms)
	{
	    tm = tm + s + " ";
	}
	bw.write(tm + "\r\n");
	bw.write("Results: ");
	for (int i = 0; i < merge.size(); i++)
	{
	    bw.write(merge.get(i) + " ");
	}
	if (merge.size() == 0)
	{
	    bw.write("empty");
	}
	bw.write("\r\nNumber of documents in results: " + merge.size() + "\r\n");
	bw.write("Number of comparisons: " + dOrcomparisons + "\n");
    }

    public static void daatAND(String[] terms) throws IOException
    {
	int dAndcomparisons = 0;
	ArrayList<LinkedList<Integer>> allpostings = new ArrayList<LinkedList<Integer>>();
	LinkedList<Integer> pointer = new LinkedList<Integer>();
	LinkedList<Integer> merge = new LinkedList<Integer>();
	int[] size = new int[terms.length];
	for (String s : terms)
	{
	    allpostings.add(dictionary.get(s));
	}
	int p = 0;
	for (LinkedList<Integer> posting : allpostings)
	{
	    size[p] = posting.size();
	    p++;
	}
	int min = size[0];
	int minlist = 0;
	for (int i = 1; i < terms.length; i++)
	{
	    if (size[i] < min)
	    {
		min = size[i];
		minlist = i;
	    }
	}
	pointer = allpostings.get(minlist);
	allpostings.add(0, pointer);
	allpostings.remove(minlist + 1);
	allpostings.sort(new Comparator<LinkedList<Integer>>()
	{
	    public int compare(LinkedList<Integer> o1, LinkedList<Integer> o2)
	    {
		if (o1.size() < o2.size())
		    return -1;
		else
		    return 1;
	    }
	});
	int docCount = 1;
	int temp = 0;
	for (int postings = 0; postings < allpostings.get(0).size(); postings++)
	{
	    docCount = 1;
	    for (int otherposting = 1; otherposting < allpostings.size(); otherposting++)
	    {
		boolean nointersection = false;
		for (int k = temp; k < allpostings.get(otherposting).size(); k++)
		{
		    dAndcomparisons++;
		    if (allpostings.get(0).get(postings) < (allpostings.get(otherposting).get(k)))
		    {
			temp = k;
			nointersection = true;
			break;
		    }
		    else if (allpostings.get(0).get(postings).equals(allpostings.get(otherposting).get(k)))
		    {
			docCount++;
			break;
		    }
		}
		if (nointersection == true)
		    break;
	    }
	    if (docCount == allpostings.size())
	    {
		merge.add(allpostings.get(0).get(postings));
	    }
	}
	bw.write("DaatAnd\r\n");
	String tm = "";
	for (String s : terms)
	{
	    tm = tm + s + " ";
	}
	bw.write(tm + "\r\n");
	bw.write("Results: ");
	for (int i = 0; i < merge.size(); i++)
	{
	    bw.write(merge.get(i) + " ");
	}
	if (merge.size() == 0)
	{
	    bw.write("empty");
	}
	bw.write("\r\nNumber of documents in results: " + merge.size() + "\r\n");
	bw.write("Number of comparisons: " + dAndcomparisons + "\n");
    }

    public static void main(String[] args) throws IOException
    {
	String indexpath = args[0];
	String outputfile = args[1];
	String inputfile = args[2];
	br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputfile)), "UTF-8"));
	bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outputfile)), "UTF-8"));
	pathobj = fs.getPath(indexpath);
	IndexReader ir = DirectoryReader.open(FSDirectory.open(pathobj));
	Collection<String> docs = MultiFields.getIndexedFields(ir);
	Iterator<String> itr = docs.iterator();
	Terms terms;
	TermsEnum termsiterator;
	PostingsEnum postingsiterator = null;
	ArrayList<String> stringlist = new ArrayList<String>();
	while (itr.hasNext())
	{
	    String element = itr.next();
	    if (element.equals("id") || element.equals("_version_"))
	    {
		continue;
	    }
	    terms = MultiFields.getTerms(ir, element);
	    termsiterator = terms.iterator();
	    while (termsiterator.next() != null)
	    {
		stringlist.add(termsiterator.term().utf8ToString());
		postingsiterator = MultiFields.getTermDocsEnum(ir, element, termsiterator.term());
		LinkedList<Integer> ll = new LinkedList<Integer>();
		while (postingsiterator.nextDoc() != PostingsEnum.NO_MORE_DOCS)
		{
		    ll.add(postingsiterator.docID());
		}
		dictionary.put(termsiterator.term().utf8ToString(), ll);
	    }
	}
	String currentline;
	while ((currentline = br.readLine()) != null)
	{
	    String[] queryterms = currentline.split(" ");
	    getPostings(queryterms);
	    taatAND(queryterms);
	    taatOR(queryterms);
	    daatAND(queryterms);
	    daatOR(queryterms);
	}
	bw.close();
	br.close();
    }
}
