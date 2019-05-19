package package1;
import java.util.regex.*;

public class Valid_Username {
   public static void main(String[]args) {
	   String s1 = "Julia";
	   String s2 = "Samantha";
	   String s3 = "Samantha_21";
	   String s4 = "1Samantha";
	   int count = 0;
//	   System.out.println(Pattern.matches("geeksforge*ks", "geeksforgeeks"));
	   Pattern p = Pattern.compile(".com$");    
	   /*  ^ab* - the string should always start from ab followed by any character.[aba,abc,abd,abcd,...]
	    *  \Acabc - matches the entire string cabc. i.e [cabc].
	    *  find() method in matcher class is used to find the any substring in a given string.
	    *  [c-d0-9A-Z] - it is used to match the uppercase,lowercase and numerical characters in string(with the OR condition).
	    *  [a-z0-9A-Z]+@gmail.com - matches the gmail ID but still some improvement is required.
	    *  [.com]$ - It is used to match the every character at the end of the string from the last
	    *  .com$ - It is used to match the entire string at the last.et u 
	    * */
	   Matcher m = p.matcher("aBC@gmail.com");     //ababbaba
	   System.out.println(m.find());
	   
/*	   while(m.find()) {
		   count++;
		   System.out.println(m.start()+","+m.end());
	   }
	   System.out.println(count);*/
   }
}
