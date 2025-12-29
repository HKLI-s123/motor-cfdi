<?php

require __DIR__ . '/vendor/autoload.php';

use PhpCfdi\SatWsDescargaMasiva\RequestBuilder\FielRequestBuilder\Fiel;
use PhpCfdi\SatWsDescargaMasiva\RequestBuilder\FielRequestBuilder\FielRequestBuilder;
use PhpCfdi\SatWsDescargaMasiva\Service;
use PhpCfdi\SatWsDescargaMasiva\WebClient\GuzzleWebClient;

class SatClient
{
    private Service $service;
    private string $rfc;
    private array $fielPaths;

    public function __construct(string $rfc)
    {
        $this->rfc = strtoupper($rfc);

        // Buscar la FIEL en las carpetas correctas
        $this->fielPaths = $this->resolveFielPaths($this->rfc);

        $cerFile = $this->fielPaths['cer'];
        $keyFile = $this->fielPaths['key'];
        $password = $this->fielPaths['password'];

        $fiel = Fiel::create(
            file_get_contents($cerFile),
            file_get_contents($keyFile),
            $password
        );

        if (! $fiel->isValid()) {
            throw new Exception("La FIEL del RFC {$this->rfc} NO es válida o no está vigente.");
        }

        $requestBuilder = new FielRequestBuilder($fiel);
        $webClient = new GuzzleWebClient();

        $this->service = new Service($requestBuilder, $webClient);
    }

    /**
     * Devuelve el servicio SAT listo para usar.
     */
    public function getService(): Service
    {
        return $this->service;
    }

    /**
     * Localiza la FIEL dentro de uploads/clientes o uploads/own-firma
     */
    private function resolveFielPaths(string $rfc): array
    {
        $base = __DIR__ . '/../uploads';

        $possibleDirs = [
            $base . '/clientes/' . $rfc,
            $base . '/own-firma/' . $rfc,
        ];

        $foundDir = null;

        foreach ($possibleDirs as $dir) {
            if (is_dir($dir)) {
                $foundDir = $dir;
                break;
            }
        }

        if (!$foundDir) {
            throw new Exception("No se encontró carpeta de FIEL para RFC {$rfc}");
        }

        // Buscar archivos por extensión
        $cerFiles = glob($foundDir . '/*.cer');
        $keyFiles = glob($foundDir . '/*.key');
        $passFiles = glob($foundDir . '/*.txt');

        if (empty($cerFiles)) {
            throw new Exception("No se encontró archivo .cer en {$foundDir}");
        }
        if (empty($keyFiles)) {
            throw new Exception("No se encontró archivo .key en {$foundDir}");
        }
        if (empty($passFiles)) {
            throw new Exception("No se encontró archivo .txt (password) en {$foundDir}");
        }

        $cerFile = $cerFiles[0];
        $keyFile = $keyFiles[0];
        $passFile = $passFiles[0];

        $password = trim(file_get_contents($passFile));

        if ($password === '') {
            throw new Exception("El archivo de contraseña .txt está vacío en {$foundDir}");
        }

        return [
            'dir' => $foundDir,
            'cer' => $cerFile,
            'key' => $keyFile,
            'password' => $password,
        ];
    }
}
