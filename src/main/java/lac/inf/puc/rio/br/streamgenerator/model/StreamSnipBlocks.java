/**
 * 
 */
package lac.inf.puc.rio.br.streamgenerator.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author vitor
 *
 */
public class StreamSnipBlocks 
{
	private List<TriplesBlock> _block;
	
	public StreamSnipBlocks()
	{
		_block = Collections.synchronizedList(new ArrayList<TriplesBlock>());
	}
	
	public void addBlock(TriplesBlock block)	
	{
		TriplesBlock newBlock = new TriplesBlock();		
		newBlock.addTriples(block.getTriples());
		
		synchronized(_block)
		{
			_block.add(newBlock);	
			if(_block.size() != 0)
				Collections.sort(_block, Comparator.comparing(TriplesBlock::getTimestamp));
		}
	}
	
	public void addBlocks(ArrayList<TriplesBlock> blocks)
	{
		for(int i = 0; i < blocks.size(); i++)
		{
			addBlock(blocks.get(i));
		}
	}
	
	public int getNumberOfBlocks()
	{		
		return _block.size();
	}
	
	public int getTotalNumberOfTriples()
	{
		int totalNumTriples = 0;
		for(int i=0; i<_block.size(); i++)
		{
			totalNumTriples = totalNumTriples + _block.get(i).size();
		}
		
		return totalNumTriples;
	}
	
	public void clear()
	{
		synchronized(_block)
		{
			_block.clear();
		}
	}
	
	public TriplesBlock getBlockWithOutDeleting(int index)
	{
		if(_block.size() == 0)
			return null;		
		
		return _block.get(index);
	}
	
	public TriplesBlock getBlock()
	{
		TriplesBlock retBlock;
		
		
		if(_block.size() == 0)
			return null;
		
		retBlock = _block.get(0);		
		_block.remove(0);
		
		return retBlock;
	}
}
