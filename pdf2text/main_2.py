from pdf2docx import Converter

pdf_file = 'D:/Dev/bio_work/pdf2text/SteamSpy.pdf'
docx_file = 'D:/Dev/bio_work/pdf2text/SteamSpy.docx'

# convert pdf to docx
cv = Converter(pdf_file)
cv.convert(docx_file, start=0, end=None)
cv.close()
