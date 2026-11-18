1. Débit (TPS) mesuré sur l'envoi réel

Problème : le TPS affiché était une moyenne depuis le démarrage du node, donc trompeuse pour des envois courts.
Logique : le TPS est maintenant calculé sur la dernière rafale de TX : de la première à la dernière TX reçue, la rafale se terminant après 1 s sans nouvelle TX. On obtient le débit réel de l'envoi.

2. Ne pas produire de blocs avant d'avoir l'historique des autres

Problème : un node relancé repart de zéro. Si c'était son tour, il créait aussitôt son propre bloc 1, différent de celui des autres. Cela créait une fourche, avec des reconnexions en boucle : expected 33, got 1.
Logique : au démarrage, le node attend avant de produire.
- Il laisse aux autres nodes le temps d'être découverts.
- Il attend que chaque pair connecté lui ait envoyé toute sa chaîne. Un message « fin d'historique » marque la fin de cet envoi.

Il rejoint donc la chaîne existante au lieu d'en créer une nouvelle.

3. Détecter une connexion morte (heartbeat)

Problème : une connexion peut mourir sans que personne s'en aperçoive, par exemple quand le VPN change de chemin. Le node qui n'avait rien à envoyer attendait indéfiniment, sans jamais se reconnecter. Il restait isolé, et la chaîne se bloquait quand c'était son tour.
Logique : chaque node envoie un Ping toutes les 2 s, et considère un pair silencieux depuis 15 s comme mort. Il coupe alors la connexion, et la reconnexion automatique prend le relais.

4. Ne jamais se connecter à soi-même

Problème : sur un partage de connexion iPhone, chaque appareil 2.0.0.2. Quand l'adresse ZeroTier d'un pair tardait à répondre,le node essayait celle-ci et tombait sur lui-même. Il se croyait connecté au pair, alors que ce pair ne recevait rien.
Logique : deux protections.
- Ces adresses partagées ne sont plus utilisées.
- Chaque connexion commence par un message de présentation avecteur : un node qui reconnaît sa propre clé ferme la connexion.

5. Outils d'observation

- Dashboard live : état de chaque node, TX reçues par seconde, u de divergence. Il a maintenant un histogramme des TX par bloc,coloré par node producteur.
- scripts/stress.sh : lance un stress test en local ou sur plusil vérifie que tous les nodes ont la même chaîne et les mêmes TX.

Ce qui reste ouvert

┌──────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────┐
│              Sujet               │                       Effet                       │                          Piste                           │
├──────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
│ Producteur absent                │ La chaîne s'arrête à son tour                     │ Passer son tour au bout d'un délai                       │
├──────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
│ Fourche déjà présente            │ Pas de réparation automatique                     │ Adopter la chaîne la plus longue                         │
├──────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
│ Gros blocs (plus de 30 000 TX)   │ Dépassent la taille maximale d'un message (16 Mo) │ Limite de TX par bloc, prise en charge par un coéquipier │
├──────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────┤
│ Réseau lent ou relayé (ZeroTier) │ Débit entre nodes limité                          │ Même réseau local de préférence                          │
└──────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────┘

