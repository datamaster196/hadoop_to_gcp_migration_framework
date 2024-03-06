CREATE OR REPLACE FUNCTION `{{var.value.INTEGRATION_PROJECT}}.udfs.udf_remove_selected_punctuation`(inputstr String)
RETURNS String
LANGUAGE js AS """

//Rule 2 - Strip all punctuation unless it’s a hyphen or an apostrophe 

    var original =""; 
	var punctuationless =""; 

	if (inputstr == null) {
	  return "";
	}
	else {
	   original = inputstr.trim();	
       punctuationless = original.replace(/[^ A-Za-z0-9_'-]/g,"");
    }
	
    return punctuationless;
""";