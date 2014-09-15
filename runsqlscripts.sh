
for languageprefix in 'en' 'ca' 'de' 'es' 'eu' 'fr' 'id' 'it' 'ja' 'ko' 'nl' 'pl' 'pt' 'ru' 'tr'
do
	cat generate_types.sql | sed -E "s/[$]prefix[$]/$languageprefix/g" | mysql -u dstype -p dstype &
done
