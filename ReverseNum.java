package module1;

import java.util.Scanner;

public class ReverseNum {
  public static void main(String[]args){
	  String s1 = "";
	  System.out.println("Enter the num");
	  Scanner kb = new Scanner(System.in);
	  float num = kb.nextFloat();
	  char []ch = Float.toString(num).toCharArray();
	  for(int i=ch.length-1;i >= 0;i--){
		 // System.out.println(ch[i]);
		  if(ch[i]=='.')
			  continue;
		  else
		      s1 = s1+ch[i];
	  }
	  System.out.print(s1);
  }
}
