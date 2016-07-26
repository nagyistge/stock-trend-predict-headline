from textblob import TextBlob
from textblob import Word
import textblob_aptagger
import codecs
import string
import sys
import re

# Initialise tagger
pt = textblob_aptagger.PerceptronTagger()


def runScript(f1, f2):
    text = codecs.open(f1, 'r', encoding='utf-8')
    text_out = open(f2, 'w')
    #text=open(sys.argv[1], 'r')
    text_lines = []

    for line in text:
        ctn = line.encode('ascii', 'ignore');
        repls = ('\n' , ''), ('            ' , ''),('^', '');
        txt = reduce(lambda a, kv: a.replace(*kv), repls, ctn);
        txt = txt.rstrip('\n')
        text_lines.append(txt);
    text.close()

    for l in text_lines:
        taggedline = ""
        l = l.rstrip('\n')
        try:
            tags = pt.tag(l)
            if len(tags) > 0:
				# for POS tag
                for word in tags: 
                    surface = word[0]
                    pos = word[1]
    #				print word
                    try:
                        if pos[0] == 'N' or pos[0] == 'V':
                            tag = Word(surface).lemmatize(
                                pos[0].lower()) + "_" + pos
                        else:
                            tag = Word(surface).lemmatize().lower() + "_" + pos
                        taggedline = taggedline + tag + " "
                    except:
                        taggedline = taggedline + surface + "_" + pos + " "
				# for Human name searching
				
        except:
            print "ERROR processing line", l

        to_print = taggedline.encode('utf8', 'replace') + "\n"
        text_out.write(to_print)
    text_out.close()

if __name__ == '__main__':
    # when executing as script
    runScript(sys.argv[1], sys.argv[2])
