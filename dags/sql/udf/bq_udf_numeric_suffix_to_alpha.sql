CREATE or REPLACE FUNCTION `{{var.value.INTEGRATION_PROJECT}}.udfs.udf_numeric_suffix_to_alpha` (inputstr String)
RETURNS String
LANGUAGE js AS """

// Rule 3 - Convert numeric suffixes to alpha (i.e. 3rd = III)

	  var reverse = function(str) {
	    var arr = [];
	    for (var i = 0, len = str.length; i <= len; i++) {
	        arr.push(str.charAt(len - i))
	    }
	    return arr.join('');
	  }

	  var roman = {"M" :1000, "CM":900, "D":500, "CD":400, "C":100, "XC":90, "L":50, "XL":40, "X":10, "IX":9, "V":5, "IV":4, "I":1};
	  var reversestr = "";  
	  var firstIndex = "";
	  var split_str = "";
	  var suffix = "";
	  var num = 0;
	  var delim = " ";
	  var str = "";
	  var convert="N"

      var inputName 
	  if (inputstr == null) {
	     return "";
	  }
	  else {
	     inputName = inputstr.trim();	
	  }	
	
	  str = inputName;

	  if (inputName.match(/(\\d+)/)) 
	  {
	     reversestr = reverse(inputName);
	     //return reversestr;
	     firstIndex = reversestr.indexOf(delim);
	     //str = inputName.reverse().substring(0, lastIndex).reverse() + delim;
	     //suffix = inputName.substring(0, lastIndex + inputName.length);
       
         if (firstIndex > 0) { 
	        suffix = reverse(reversestr.substring(0, firstIndex));
	        str = reverse(reversestr.slice(reversestr.indexOf(delim) + delim.length)) + delim;
	        split_str = suffix.replace(/\\D/g, '');
	        //return split_str;
	        num = parseInt(split_str) //Extract number from right of input string					
			convert = "Y";
         }
         else // No conversion. Return string as is.
         {
          str = reverse(reversestr);
		  firstchar = str.charAt(0);
		  if (firstchar.match(/(\\d+)/)) 
		  {
           convert = "Y";
	       split_str = str.replace(/\\D/g, '');
	       //return split_str;
	       num = parseInt(split_str); //Extract number from right of input string
           str = "";
		  }		  
          else 
		  {
             convert = "N";
			 num = 0;		     
		  }
        }

	     //return firstIndex
	     //return suffix;
	     //return str;

	  //var num = 3
      if (convert == "Y") { // When Okay to convert

        if (num > 999)  // support suffix conversion for value up to 999. Beyond value 999: return empty string for suffix.
        {
          num = 0;
        }
		for (var i of Object.keys(roman) ) 
	    {
		  var q = Math.floor(num / roman[i]);
		  num -= q * roman[i];
		  str += i.repeat(q);
		}
	  }
	 }
		return str; //Return the converted string
	
""";