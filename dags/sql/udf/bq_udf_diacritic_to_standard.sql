CREATE or REPLACE FUNCTION `{{var.value.INTEGRATION_PROJECT}}.udfs.udf_diacritic_to_standard`(inputstr String)
RETURNS String
LANGUAGE js AS """

function titleCase(str) {
  str = str.toLowerCase().split(' ');
  for (var i = 0; i < str.length; i++) {
    str[i] = str[i].charAt(0).toUpperCase() + str[i].slice(1); 
  }
  return str.join(' ');
}

   var replacechars={ "œ":"ce" , "ÿ":"y" , "ç":"c",  "æ":"ae", "ù":"u", "ú":"u", "û":"u", "ü":"u",  "è":"e", "é":"e", "ê":"e", "ë":"e", "ò":"o", "ó":"o", "ô":"o", "ö":"o", "ø":"o", "ì":"i", "í":"i", "î":"i", "ï":"i", "à":"a", "á":"a", "â":"a", "ä":"a", "å":"a"};

    var standardname 
	if (inputstr == null) {
	  return "";
	}
	else {
	  standardname = inputstr.trim();	
	}	
	
    for (var x in replacechars) {
        standardname = standardname.replace(new RegExp(x, 'gi'), replacechars[x]);
        var init_standardname = titleCase(standardname)
    }
    return init_standardname;
    
"""