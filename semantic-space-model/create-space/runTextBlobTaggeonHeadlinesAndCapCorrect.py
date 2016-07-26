from textblob import TextBlob
from textblob import Word
import textblob_aptagger
import codecs
import string
import sys
import re
import nltk;
from nltk import pos_tag, word_tokenize;
from nltk.corpus import stopwords;

# Initialise tagger
pt = textblob_aptagger.PerceptronTagger()


def runScript(f1, f2):
    text = codecs.open(f1, 'r', encoding='utf-8')
    open(sys.argv[2], 'w').close(); # Earase the output file
    text_out = open(f2, 'w') # Reopen the output for writing
    #text=open(sys.argv[1], 'r')
    text_lines = []
    count = 0;
    # Get stopword list
    stop_words = set(stopwords.words('english'));
    stop_words.update(['.', ',', '"', "'", '?', '!', ':', ';', '(', ')', '[', ']', '{', '}','n\'t']);
    for line in text:        
        ctn = line.encode('ascii', 'ignore');
        repls = ('\n' , ''), ('            ' , ''),('^', '');
        txt = reduce(lambda a, kv: a.replace(*kv), repls, ctn);
        txt = txt.rstrip('\n')
        text_lines.append(txt);
    text.close()

    for l in text_lines:
        count+=1;
        if (count%500 == 0):
            print "processed {0:10}".format(count);
        taggedline = ""
        l = l.rstrip('\n')
        try:
            tags = pt.tag(l)
            if len(tags) > 1:
				
                # Run NLTK tag as reference
                nltk_tags= nltk.pos_tag(word_tokenize(l));
                if len(nltk_tags) == len(tags):
                    #Pair-wise compare the tag
                    diff_count=0; # count number of different tags from the both 2 tagger
                    sens="";    # new string, that contain corrected word
                    for i in xrange(0,len(tags)):
                        if tags[i][1] == nltk_tags[i][1]:
                            sens = sens + ' ' + tags[i][0];
                        else: # the two tagger did not agreed
                            sens = sens + ' ' + tags[i][0].lower(); # Lowwer case it
                            diff_count+=1;
                    if (diff_count>1):
#                        print sens, l;
                        tags = pt.tag(sens);
                # for POS tag
                for i in xrange(0,len(tags)): 
                    word =   tags[i]                  
                    surface = re.sub(r'^[^a-zA-Z_]', "", word[0]);
                    if ((surface.lower() in stop_words) or len(surface)<3):
                        continue;
                    pos = word[1]
    #				print word
                    try:
                        if pos[0] == 'N' or pos[0] == 'V':
                            tag = Word(surface).lemmatize(
                                pos[0].lower());
                        else:
                            tag = Word(surface).lemmatize().lower()
                            
                        taggedline = taggedline + tag + " "
                    except:
                        taggedline = taggedline + surface + " "
				# for Human name searching
				
        except:
            print "ERROR processing line", l

        to_print = taggedline.encode('utf8', 'replace') + "\n"
        if len(to_print)<1: print len(to_print);
        text_out.write(to_print)
    text_out.close()

if __name__ == '__main__':
    # when executing as script
    runScript(sys.argv[1], sys.argv[2])
