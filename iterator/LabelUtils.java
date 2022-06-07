package iterator;


import heap.*;
import labelheap.*;
import global.*;
import java.io.*;
import java.lang.*;

/**
 *some useful method when processing Label 
 */
public class LabelUtils
{
  
  /**
   * This function compares a label with another label in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the label is greater,
   *   -1        if the label is smaller,
   *
   *@param    fldType   the type of the field being compared.
   *@param    l1        one label.
   *@param    l2        another label.
   *@param    l1_fld_no the field numbers in the label to be compared.
   *@param    l2_fld_no the field numbers in the label to be compared. 
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception LabelUtilsExcelption exception from this class
   *@return   0        if the two are equal,
   *          1        if the label is greater,
   *         -1        if the label is smaller,                              
   */
  public static int CompareLabelWithLabel(AttrType fldType,
					  Label  l1, int l1_fld_no,
					  Label  l2, int l2_fld_no)
    throws IOException,
	   UnknowAttrType,
	   LabelUtilsException
    {
      int   l1_i,  l2_i;
      float l1_r,  l2_r;
      String l1_s, l2_s;
      
      switch (fldType.attrType) 
	{
	case AttrType.attrInteger:                // Compare two integers.
	  try {
	    l1_i = l1.getIntFld(l1_fld_no);
	    l2_i = l2.getIntFld(l2_fld_no);
	  }catch (LabelFieldNumberOutOfBoundException e){
	    throw new LabelUtilsException(e, "FieldNumberOutOfBoundException is caught by LabelUtils.java");
	  }
	  if (l1_i == l2_i) return  0;
	  if (l1_i <  l2_i) return -1;
	  if (l1_i >  l2_i) return  1;
	  
	case AttrType.attrReal:                // Compare two floats
	  try {
	    l1_r = l1.getFloFld(l1_fld_no);
	    l2_r = l2.getFloFld(l2_fld_no);
	  }catch (LabelFieldNumberOutOfBoundException e){
	    throw new LabelUtilsException(e, "LabelFieldNumberOutOfBoundException is caught by LabelUtils.java");
	  }
	  if (l1_r == l2_r) return  0;
	  if (l1_r <  l2_r) return -1;
	  if (l1_r >  l2_r) return  1;
	  
	case AttrType.attrString:                // Compare two strings
	  try {
	    l1_s = l1.getStrFld(l1_fld_no);
	    l2_s = l2.getStrFld(l2_fld_no);
	  }catch (LabelFieldNumberOutOfBoundException e){
	    throw new LabelUtilsException(e, "LabelFieldNumberOutOfBoundException is caught by LabelUtils.java");
	  }
	  
	  // Now handle the special case that is posed by the max_values for strings...
	  if(l1_s.compareTo( l2_s)>0)return 1;
	  if (l1_s.compareTo( l2_s)<0)return -1;
	  return 0;
	default:
	  
	  throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
	  
	}
    }
  
  
  
  /**
   * This function  compares  label1 with another label2 whose
   * field number is same as the label1
   *
   *@param    fldType   the type of the field being compared.
   *@param    l1        one label
   *@param    value     another label.
   *@param    l1_fld_no the field numbers in the labels to be compared.  
   *@return   0        if the two are equal,
   *          1        if the label is greater,
   *         -1        if the label is smaller,  
   *@exception UnknowAttrType don't know the attribute type   
   *@exception IOException some I/O fault
   *@exception LabelUtilsException exception from this class   
   */            
  public static int CompareLabelWithValue(AttrType fldType,
					  Label  l1, int l1_fld_no,
					  Label  value)
    throws IOException,
	   UnknowAttrType,
	   LabelUtilsException
    {
      return CompareLabelWithLabel(fldType, l1, l1_fld_no, value, l1_fld_no);
    }
  
  /**
   *This function Compares two Label in all fields 
   * @param l1 the first label
   * @param l2 the secocnd label
   * @param type[] the field types
   * @param len the field numbers
   * @return  0        if the two are not equal,
   *          1        if the two are equal,
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception LabelUtilsException exception from this class
   */            
  
  public static boolean Equal(Label t1, Label t2, AttrType types[], int len)
    throws IOException,UnknowAttrType,LabelUtilsException
    {
      int i;
      
      for (i = 1; i <= len; i++)
	if (CompareLabelWithLabel(types[i-1], t1, i, t2, i) != 0)
	  return false;
      return true;
    }
  
  /**
   *get the string specified by the field number
   *@param label the label 
   *@param fidno the field number
   *@return the content of the field number
   *@exception IOException some I/O fault
   *@exception LabelUtilsException exception from this class
   */
  public static String Value(Label label, int fldno)
    throws IOException,
	   LabelUtilsException
    {
      String temp;
      try{
	temp = label.getStrFld(fldno);
      }catch (LabelFieldNumberOutOfBoundException e){
	throw new LabelUtilsException(e, "LabelFieldNumberOutOfBoundException is caught by LabelUtils.java");
      }
      return temp;
    }
  
 
  /**
   *set up a label in specified field from a label
   *@param value the label to be set 
   *@param label the given label
   *@param fld_no the field number
   *@param fldType the label attr type
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception LabelUtilsException exception from this class
   */  
  public static void SetValue(Label value, Label  label, int fld_no, AttrType fldType)
    throws IOException,
	   UnknowAttrType,
	   LabelUtilsException
    {
      
      switch (fldType.attrType)
	{
	case AttrType.attrInteger:
	  try {
	    value.setIntFld(fld_no, label.getIntFld(fld_no));
	  }catch (LabelFieldNumberOutOfBoundException e){
	    throw new LabelUtilsException(e, "LabelFieldNumberOutOfBoundException is caught by LabelUtils.java");
	  }
	  break;
	case AttrType.attrReal:
	  try {
	    value.setFloFld(fld_no, label.getFloFld(fld_no));
	  }catch (LabelFieldNumberOutOfBoundException e){
	    throw new LabelUtilsException(e, "LabelFieldNumberOutOfBoundException is caught by LabelUtils.java");
	  }
	  break;
	case AttrType.attrString:
	  try {
	    value.setStrFld(fld_no, label.getStrFld(fld_no));
	  }catch (LabelFieldNumberOutOfBoundException e){
	    throw new LabelUtilsException(e, "LabelFieldNumberOutOfBoundException is caught by LabelUtils.java");
	  }
	  break;
	default:
	  throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
	  
	}
      
      return;
    }
  
  
  /**
   *set up the Jlabel's attrtype, string size,field number for using join
   *@param Jlabel  reference to an actual label  - no memory has been malloced
   *@param res_attrs  attributes type of result label
   *@param in1  array of the attributes of the label (ok)
   *@param len_in1  num of attributes of in1
   *@param in2  array of the attributes of the label (ok)
   *@param len_in2  num of attributes of in2
   *@param l1_str_sizes shows the length of the string fields in S
   *@param l2_str_sizes shows the length of the string fields in R
   *@param proj_list shows what input fields go where in the output label
   *@param nOutFlds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception LabelUtilsException exception from this class
   */
  public static short[] setup_op_label(Label Jlabel, AttrType[] res_attrs,
				       AttrType in1[], int len_in1, AttrType in2[], 
				       int len_in2, short l1_str_sizes[], 
				       short l2_str_sizes[], 
				       FldSpec proj_list[], int nOutFlds)
    throws IOException,
	   LabelUtilsException
    {
      short [] sizesL1 = new short [len_in1];
      short [] sizesL2 = new short [len_in2];
      int i, count = 0;
      
      for (i = 0; i < len_in1; i++)
        if (in1[i].attrType == AttrType.attrString)
	  sizesL1[i] = l1_str_sizes[count++];
      
      for (count = 0, i = 0; i < len_in2; i++)
	if (in2[i].attrType == AttrType.attrString)
	  sizesL2[i] = l2_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer)
	    res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
	  else if (proj_list[i].relation.key == RelSpec.innerRel)
	    res_attrs[i] = new AttrType(in2[proj_list[i].offset-1].attrType);
	}
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
            n_strs++;
	  else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType == AttrType.attrString)
            n_strs++;
	}
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
            res_str_sizes[count++] = sizesL1[proj_list[i].offset-1];
	  else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType ==AttrType.attrString)
            res_str_sizes[count++] = sizesL2[proj_list[i].offset-1];
	}
      try {
        Jlabel.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
      }catch (Exception e){
	throw new LabelUtilsException(e,"setHdr() failed");
      }
      return res_str_sizes;
    }
  
 
   /**
   *set up the Jlabel's attrtype, string size,field number for using project
   *@param Jlabel  reference to an actual label  - no memory has been malloced
   *@param res_attrs  attributes type of result label
   *@param in1  array of the attributes of the label (ok)
   *@param len_in1  num of attributes of in1
   *@param l1_str_sizes shows the length of the string fields in S
   *@param proj_list shows what input fields go where in the output label
   *@param nOutFlds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception LabelUtilsException exception from this class
   *@exception InvalidRelation invalid relation 
   */

  public static short[] setup_op_label(Label Jlabel, AttrType res_attrs[],
				       AttrType in1[], int len_in1,
				       short l1_str_sizes[], 
				       FldSpec proj_list[], int nOutFlds)
    throws IOException,
	   LabelUtilsException, 
	   InvalidRelation
    {
      short [] sizesL1 = new short [len_in1];
      int i, count = 0;
      
      for (i = 0; i < len_in1; i++)
        if (in1[i].attrType == AttrType.attrString)
	  sizesL1[i] = l1_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer) 
            res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
	  
	  else throw new InvalidRelation("Invalid relation -innerRel");
	}
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer
	      && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
	    n_strs++;
	}
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++) {
	if (proj_list[i].relation.key ==RelSpec.outer
	    && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
	  res_str_sizes[count++] = sizesL1[proj_list[i].offset-1];
      }
     
      try {
	Jlabel.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
      }catch (Exception e){
	throw new LabelUtilsException(e,"setHdr() failed");
      } 
      return res_str_sizes;
    }
}



