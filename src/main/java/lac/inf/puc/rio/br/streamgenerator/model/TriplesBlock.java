/**
 * 
 */
package lac.inf.puc.rio.br.streamgenerator.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import eu.larkc.csparql.cep.api.RdfQuadruple;

/**
 * @author vitor
 *
 */
public class TriplesBlock
{
	private List<RdfQuadruple> _triples = null;

	public TriplesBlock()
	{			
		_triples = new ArrayList<RdfQuadruple>();
	}
	
	/**
	 * Retorna o timestamp das triplas deste bloco.
	 * @return
	 */
	public Long getTimestamp()
	{
		if(_triples != null && _triples.size() != 0)
			return _triples.get(0).getTimestamp();
		else
			return 0L;
	}
	
	public List<RdfQuadruple> getTriples()
	{
		return _triples;
	}
	
	public int size()
	{
		return _triples.size();
	}
	
	public void clear()
	{
		synchronized(_triples)
		{
			_triples.clear();
		}
	}
	
	public void addTriples(List<RdfQuadruple> triples)
	{		
		_triples.addAll(triples);
	}
	
	public void addTriple(RdfQuadruple triple)
	{	
		_triples.add(triple);
	}
		
}
